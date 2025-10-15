---
configs:
  - config_name: default
    data_files:
    - split: train
      path: tramites.csv
license: cc0-1.0
language:
  - es
tags:
  - legal
  - finance
pretty_name: trámites-bo del portal www.gob.bo
size_categories:
  - 1M<n<50M

---

Análisis de datos de trámites en entidades del gobierno boliviano tomados directamente del [portal oficial](https://gob.bo) y publicados como datos abiertos y de libre uso. Este es un trabajo derivado del proyecto [https://github.com/datosbolivia/tramites-bo](https://github.com/datosbolivia/tramites-bo).

Incluye:

- [Datos de cada trámite en formato csv](tramites.csv). Generado a partir de tramites.jsonl y el [notebooks/preprocessing_1.ipynb](notebooks/preprocessing_1.ipynb)
- [Datos de trámites desglosados para facilitar su análisis](tramites_desglosados.csv). Generado desde tramites.csv y el notebook [notebooks/Desglose.ipynb](notebooks/Desglose.ipynb)
- Especificación de los datos en [datapackage.json](datapackage.json)
- [Datos de cada trámite en su forma original en formato jsonl](tramites.jsonl). Obtenidos a partir del proyecto [tramites-bo](https://github.com/datosbolivia/tramites-bo).
