# Proyecto 04 — NYC TLC: RAW → OBT → ML

Este repo contiene la infraestructura y artefactos para:
- Ingestar Parquet 2015–2025 (Yellow/Green) a Postgres (esquema `raw`).
- Construir/actualizar la One Big Table `analytics.obt_trips` con un servicio CLI (`obt-builder`).
- Entrenar y comparar modelos lineales regularizados para predecir `total_amount` (from‑scratch vs scikit‑learn) en el notebook `ml_total_amount_regression.ipynb`.

La entrega cumple con el documento `proyecto04.md` (requisitos, rúbrica y checklist).

Nota importante (estructura): La infraestructura ejecutable y autocontenida está en la carpeta `Proyecto Completo/`. Ahí se encuentran `docker-compose.yml`, `notebooks/`, `obt-builder/` y `spark-notebook.Dockerfile`. Ejecuta todos los comandos desde esa carpeta.

## Variables de entorno

Crear un archivo `.env` (usar como base `.env.example`) con:

- Postgres:
  - `PG_HOST` 
  - `PG_PORT` 
  - `PG_DB`
  - `PG_USER` 
  - `PG_PASSWORD`
  - `PG_SCHEMA_RAW=raw`
  - `PG_SCHEMA_ANALYTICS=analytics`
- Parámetros de ingesta/obt-builder (opcionales por defecto):
  - `YEARS=2015-2025`
  - `SERVICES=yellow,green`
  - `RUN_ID=dev_run`
  - Bandas opcionales usadas en notebooks de ingesta: `CHECK_EXISTS=1`, `COUNT_ROWS=0`, `CREATE_INGESTED_IDX=0`.

## Levantar servicios

1) Construir imágenes y levantar Postgres y (opcional) pgAdmin:
- Postgres expone `5432` y persiste datos en `./pgdata` (volumen del repo)
- pgAdmin (opcional) expone `5050`

2) Levantar el contenedor de Jupyter+Spark:
- La imagen se construye con `spark-notebook.Dockerfile` y monta `./notebooks` en `/home/jovyan/work`.
- Incluye el driver JDBC de Postgres.

3) Abrir y correr los notebooks:
- Abre Jupyter en http://localhost:8888 (el token aparece en los logs del contenedor `spark-notebook`).
- Notebook de ingesta: `notebooks/01_ingesta_parquet_raw.ipynb`.
- Notebook de ML: `notebooks/ml_total_amount_regression.ipynb`.

## Ingesta RAW (Spark → Postgres)

Notebook: `notebooks/01_ingesta_parquet_raw.ipynb` (dentro de `Proyecto Completo/`)
- Descarga bajo demanda cada Parquet y evita staging masivo.
- Escribe en `raw.yellow_taxi_trip`, `raw.green_taxi_trip` y `raw.taxi_zone_lookup`.
- Metadatos por lote: `service_type`, `source_year`, `source_month`, `ingested_at_utc`.
- Idempotencia: verificación por partición (EXISTS con `(source_year, source_month)`), índices opcionales y `ANALYZE`.

Notas:
- La celda de “prueba” ingesta un ejemplo para validar; para el backfill completo 2015–2025, usa los bucles provistos (Yellow y Green) dentro del mismo notebook.

## Construcción de OBT (obt-builder)

Servicio: `obt-builder` (carpeta `obt-builder/` dentro de `Proyecto Completo/`). Entrypoint: `python build_obt.py`

- Comando de evaluación (recomendado por el profesor; ejecutar desde `Proyecto Completo/`):
  - `docker compose run --rm obt-builder --full-rebuild --run-id OBT_$(Get-Date -UFormat %Y%m%d_%H%M%S)`
  - Atajo `--full-rebuild` = modo `full`, años 2015–2025, `services=yellow,green`, `overwrite=true`, `months=1..12`.
- Modo manual (by‑partition):
  - `docker compose run --rm obt-builder --mode by-partition --year-start 2020 --year-end 2020 --months 6,7,8 --services yellow,green --run-id 2020_summer --overwrite false`
- Diagnóstico de entorno (no ejecuta SQL):
  - `docker compose run --rm obt-builder --diagnose-env`

Características del script (`obt-builder/build_obt.py`):
- Usa exclusivamente variables `PG_*` y verifica que existan.
- Crea `schemas` y la tabla `analytics.obt_trips` si no existen.
- Inserta por servicio/año/mes con join a `raw.taxi_zone_lookup` y deriva: `trip_duration_min`, `avg_speed_mph`, `tip_pct`.
- Idempotencia por partición: omite si ya existe o borra previo si `--overwrite true`.
- Logs claros por partición: filas insertadas y tiempo, más un resumen final.

## Modelado — ML (from‑scratch vs scikit‑learn)

Notebook: `notebooks/ml_total_amount_regression.ipynb` (dentro de `Proyecto Completo/`)
- Target: `total_amount` (sin leakage; solo features disponibles en pickup)
- Split temporal: Train (años viejos), Validación (intermedio), Test (reciente)
- Preprocesamiento común: imputación, escalado, OHE y `PolynomialFeatures` (en 2–3 numéricas claves)
- From‑scratch (NumPy): `SGD` con penalización `none/L1/L2/elasticnet` (clase `SGDLinearRegressor`)
- scikit‑learn equivalentes: `SGDRegressor`, `Ridge`, `Lasso`, `ElasticNet`
- Tuning comparable (grillas pequeñas), mismas seeds y features
- Comparativa obligatoria: tabla de RMSE/MAE/R² en Validación y Test, con tiempos y conteo de coeficientes
- Evidencias exportadas a `notebooks/artifacts/` (métricas, pipeline ganador, etc.)

## Evidencias

Directorio `notebooks/artifacts/` contiene:
- `metadata.json` y `metadata_extended.json` (config, features, métricas)
- `metrics_winner.json` (selección final)
- `winner_pipeline.joblib` (si el ganador es sklearn)
- `model_pipeline.joblib` (último pipeline entrenado)

Nota: si re‑ejecutas el notebook y mejoras métricas, sustituye los JSON con los valores finales antes de entregar.

## Comandos típicos (PowerShell)

Estos comandos son orientativos; ya están documentados arriba, pero los dejamos aquí juntos. Asegurarse de estar en la carpeta `Proyecto Completo/` antes de ejecutarlos.

- Levantar Postgres y pgAdmin (opcional):
  - `docker compose up -d postgres pgadmin`
- Construir y levantar spark‑notebook:
  - `docker compose build spark-notebook`
  - `docker compose up -d spark-notebook`
- Construir OBT full (2015–2025):
  - `docker compose run --rm obt-builder --full-rebuild --run-id OBT_$(Get-Date -UFormat %Y%m%d_%H%M%S)`
- Construcción OBT por partición:
  - `docker compose run --rm obt-builder --mode by-partition --year-start 2023 --year-end 2023 --months 1,2,3 --services yellow,green --run-id test_run --overwrite false`

- Abrir Jupyter y correr los notebooks:
  - `docker compose logs -f spark-notebook`  # para copiar el token
  - Navega a: http://localhost:8888 y abre `notebooks/01_ingesta_parquet_raw.ipynb` y `notebooks/ml_total_amount_regression.ipynb`

## Checklist (auto)

- [x] RAW en Postgres: `raw.yellow_taxi_trip`, `raw.green_taxi_trip`, `raw.taxi_zone_lookup` (metadatos por lote)
- [x] OBT `analytics.obt_trips` construida por `obt-builder` (comando reproducible, logs por partición)
- [x] 4 modelos from‑scratch + 4 sklearn, misma `pipeline` de preprocesamiento y split
- [x] Comparativa: tabla RMSE/MAE/R² (Validación y Test) + tiempos
- [x] Diagnóstico: residuales y errores por buckets
- [x] `.env.example`, seeds fijas, comandos claros

## Problemas conocidos

- Los JSON en `notebooks/artifacts` pueden quedar desactualizados respecto a la última corrida. Asegúrate de exportar los resultados finales (especialmente RMSE en Test) antes de entregar.
- Si tu RMSE en Test cae por debajo del baseline, revisa filtrado de outliers, capping de `trip_distance`, flags (pico/fin de semana) y la cardinalidad de categóricas (Top‑K + “Other”).

### Evidencias
Los resultados y métricas del modelo ganador se encuentran en `notebooks/artifacts/`:
- `metrics_winner.json` – RMSE/MAE/R² finales en test.
- `metadata.json` – configuración del pipeline y features.
- `errors_by_trip_distance_decile.csv` – errores por decil de distancia.
- `errors_by_pickup_hour.csv` – errores por hora de pickup.
