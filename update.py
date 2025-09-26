#!/usr/bin/env python

import requests
import httpx
import asyncio
from tqdm import tqdm
import jsonlines
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path
from utils import async_httpx_retry
from deepdiff import DeepDiff
import json


def listarTramites(pageSize=30):
    """
    Lista todos los trámites disponibles.
    """

    print("Listando trámites ...")
    tramites = []
    page = 1
    while True:
        try:
            url = f"https://www.gob.bo/ws/api/portal/tramites?pagina={page}&limite={pageSize}"
            response = requests.get(url)
            datos = response.json()["datos"]
            tramites.extend(
                [{k: e[k] for k in ["id", "nombre", "slug"]} for e in datos["filas"]]
            )
            print(f"{len(tramites)} de {datos['total']}")
            if len(tramites) >= datos["total"]:
                break
            else:
                page += 1
        except Exception as e:
            print(f"{e}")
    tramites = list({d["slug"]: d for d in tramites}.values())
    return tramites


@async_httpx_retry(max_retries=5, base_delay=0.5)
async def getTramite(tramite_slug, client):
    """
    Descarga datos de un trámite.
    """
    url = f"https://www.gob.bo/ws/api/portal/tramites/{tramite_slug}"
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


async def getTramites(tramitesListado, max_concurrent=10, max_tramites=None):
    """
    Descarga asíncrona de datos de trámites en un listado.
    """
    tramites = []
    errores = []
    sema = asyncio.Semaphore(max_concurrent)
    subset = tramitesListado if max_tramites is None else tramitesListado[:max_tramites]
    pbar = tqdm(total=len(tramitesListado), desc="Descargando trámites")

    async def fetch_one(tramite, client):
        async with sema:
            try:
                data = await getTramite(tramite["slug"], client)
                tramites.append(data["datos"])
            except Exception as e:
                print(e)
                errores.append({**tramite, "error": str(e)})
            pbar.update(1)

    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}) as client:
        await asyncio.gather(*(fetch_one(t, client) for t in subset))

    pbar.close()
    return tramites, errores


def detectarModificaciones(df1, df2, timestamp):
    """
    Detecta trámites que cambian entre dos corridas
    consecutivas df1 y df2. Construye y guarda una
    bitácora de estos trámites más una estampa de tiempo.
    """

    def listarCamposCompuestos():
        """
        Listar campos cuyos valores esperamos
        que sean arrays u objetos.
        """
        with open("datapackage.json", "r") as f:
            datapackage = json.load(f)
        return [
            field["name"]
            for field in datapackage["resources"][0]["schema"]["fields"]
            if field["type"] in ["array", "object"]
        ]

    FILENAME = Path("modificaciones.csv")

    # Alinear trámites
    _df1, _df2 = df1.set_index("id").copy(), df2.set_index("id").copy()
    nombres = _df1.nombre.to_dict()
    entidades = _df1["entidad"].apply(lambda _: _["nombre"]).to_dict()
    cols = [c for c in _df1.columns if c in _df2.columns and c != "id"]
    idx = _df1.index.intersection(_df2.index)
    _df1, _df2 = _df1.loc[idx, cols], _df2.loc[idx, cols]

    # Comparar cada columna e identificar cambios
    cambios = []
    camposCompuestos = listarCamposCompuestos()
    for col in cols:
        old, new = _df1[col], _df2[col]
        modified = old.ne(new) & ~(old.isna() & new.isna())

        # Si existen modificaciones
        if modified.any():
            # Si los valores son arrays u objetos
            if col in camposCompuestos:
                for id_tramite, v1, v2 in zip(
                    old.index[modified].values,
                    old[modified].values,
                    new[modified].values,
                ):
                    # Detectar cambios detallados y agregar cada uno en una fila
                    diff = DeepDiff(v1, v2)
                    for key in diff["values_changed"].keys():
                        campo = f"{col}{key.replace('root', '')}"
                        viejo, nuevo = [
                            diff["values_changed"][key][v]
                            for v in ["old_value", "new_value"]
                        ]
                        cambios.append(
                            {
                                "timestamp": timestamp,
                                "id": id_tramite,
                                "entidad": entidades[id_tramite],
                                "nombre": nombres[id_tramite],
                                "campo": campo,
                                "viejo": viejo,
                                "nuevo": nuevo,
                            }
                        )
            else:
                # Si los valores son simples
                for id_tramite, viejo, nuevo in zip(
                    old.index[modified].values,
                    old[modified].values,
                    new[modified].values,
                ):
                    cambios.append(
                        {
                            "timestamp": timestamp,
                            "id": id_tramite,
                            "entidad": entidades[id_tramite],
                            "nombre": nombres[id_tramite],
                            "campo": col,
                            "viejo": viejo,
                            "nuevo": nuevo,
                        }
                    )

    # Guardar cambios
    print(f"{len(cambios)} modificaciones")
    if len(cambios) > 0:
        modificaciones = pd.DataFrame(cambios)
        if FILENAME.exists():
            modificaciones = pd.concat([pd.read_csv(FILENAME), modificaciones])
        modificaciones.sort_values(["timestamp", "id", "campo"]).to_csv(
            FILENAME, index=False
        )


def detectarAdiciones(df1, df2, timestamp):
    """
    Detecta trámites que aparecen o desaparecen
    entre dos corridas consecutivas df1 y df2.
    Construye y guarda una bitácora de estos trámites
    más una estampa de tiempo.
    """

    FILENAME = Path("adiciones.csv")

    # El formato de la bitácora
    def formatear(df, evento, timestamp):
        n = df[["id", "entidad", "nombre"]].copy()
        n["entidad"] = n["entidad"].str["nombre"]
        n.columns = ["id", "entidad", "nombre"]
        n.insert(0, "tipo", evento)
        n.insert(0, "timestamp", timestamp)
        return n

    # Detectar trámites que aparecen o desaparecen
    eventos = pd.concat(
        [
            formatear(df2[~df2["id"].isin(df1["id"])], "aparece", timestamp),
            formatear(df1[~df1["id"].isin(df2["id"])], "desaparece", timestamp),
        ]
    )

    # Guardar registros
    print(f"{eventos.shape[0]} trámites que aparecen o desaparecen")
    if eventos.shape[0] > 0:
        if FILENAME.exists():
            eventos = pd.concat([pd.read_csv(FILENAME), eventos])

        eventos.sort_values(["timestamp", "id", "tipo"]).to_csv(FILENAME, index=False)


async def main():
    """
    Lista todos los trámites disponibles y descarga
    datos para cada uno en una serie de reintentos.
    Luego guarda todos estos datos más posibles errores
    junto a bitácoras de trámites que aparecen, desaparecen
    o son modificados entre corridas consecutivas.
    """

    FILENAME = Path("tramites.jsonl")

    # Listar tramites
    pendientes = listarTramites()
    print(f"{len(pendientes)} tramites listados")

    # Estampa de tiempo
    timestamp = datetime.now(timezone.utc).isoformat(timespec="minutes")

    tramites, errores = await getTramites(pendientes)

    print(f"{len(tramites)} registros, {len(errores)} errores")

    # Consolidar con datos recogidos previamente
    if FILENAME.exists():
        tramites_df = pd.DataFrame(tramites)
        with jsonlines.open(FILENAME, "r") as f:
            tramites_previos = pd.DataFrame([line for line in f])

        detectarAdiciones(tramites_previos, tramites_df, timestamp)
        detectarModificaciones(tramites_previos, tramites_df, timestamp)

    # Guardar trámites y errores
    tramites_sorted = sorted(tramites, key=lambda d: d["id"])
    for data, filename in zip([tramites_sorted, errores], ["tramites", "errores"]):
        if data:
            with jsonlines.open(f"{filename}.jsonl", "w") as f:
                for entry in data:
                    f.write(entry)
    print(f"Datos guardados: {len(tramites_sorted)} trámites | {len(errores)} errores.")


if __name__ == "__main__":
    asyncio.run(main())
