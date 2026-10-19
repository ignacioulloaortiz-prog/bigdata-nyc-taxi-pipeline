# Bitácora del proyecto — hitos e incidencias

## Roles y responsabilidades

Trabajo **individual** (Ignacio Ulloa). Las responsabilidades se separaron en cuatro roles para planificar y controlar el avance como si fuera un equipo:

| Rol | Responsabilidades | Fortaleza aprovechada |
|---|---|---|
| Líder de proyecto | Lectura de la pauta, plan y Kanban, control de hitos, informe y entrega | Organización y redacción |
| Arquitecto cloud | Elección del stack Free Tier, diagramas, alta disponibilidad, BigQuery, IAM | Conocimiento de servicios cloud |
| Ingeniero de datos | Ingesta, Data Lake por zonas, ETL PySpark, calidad de datos, scripts | Programación en Python/Spark |
| Científico de datos | EDA distribuido, modelo predictivo, validación e interpretación | Estadística y ML |

## Hitos

| Hito | Criterio de logro | Estado |
|---|---|---|
| H1 Alcance definido | Stack, dataset y problema de negocio elegidos | ✅ Logrado |
| H2 Data Lake operativo | Zonas raw/bronze/silver/gold escritas en Parquet particionado | ✅ Logrado |
| H3 Modelo validado | GBT con CV 3-fold y prueba temporal sobre marzo | ✅ Logrado |
| H4 Entrega | Informe PDF + repositorio GitHub publicados | ⏳ En curso |

## Registro de incidencias y bloqueos

| # | Incidencia / bloqueo | Impacto | Solución aplicada |
|---|---|---|---|
| I-01 | El CDN oficial de NYC TLC (cloudfront) respondió **403** desde el entorno de ejecución | Bloqueaba la ingesta | Se usó el espejo público de DataTalksClub en GitHub Releases (mismos archivos oficiales en CSV.gz). La URL queda parametrizada en `config/config.yaml` |
| I-02 | `pip install pyspark` falló al compilar la *wheel* con el setuptools del sistema (Python 3.13) | No se podía crear la sesión Spark | Entorno virtual aislado con `pip/setuptools/wheel` actualizados y `pyspark==3.5.3` fijado en `requirements.txt` |
| I-03 | `VectorAssembler` falló con *"Encountered null while assembling a row"* | El entrenamiento se detenía | Las variables `is_airport` y `same_borough` daban NULL cuando la zona no tenía borough; se corrigió con `coalesce(..., 0)` en la capa SILVER y se agregó `Imputer` para `passenger_count` |
| I-04 | Recursos limitados del Free Tier (≈12 GB RAM y 2 vCPU en Colab) | Riesgo de quedarse sin memoria con GBT + CV sobre 3 M de filas | Entrenamiento sobre muestra aleatoria del 25% de ene–feb, CV 3-fold sobre sub-muestra, evaluación sobre **marzo completo**; `cache()` solo de lo necesario |
| I-05 | Lectura de CSV.gz lenta (formato no divisible: 1 tarea por archivo) | ETL lento y sin paralelismo | Conversión inmediata a Parquet particionado (divisible, columnar, comprimido) en BRONZE; el resto del pipeline solo lee Parquet |
| I-07 | En BigQuery Sandbox las particiones por fecha expiran a los 60 días de su fecha | Datos de 2021 se habrían borrado al cargarse | Partición por rango entero sobre `month` + clustering por fecha; timestamps Parquet como `TIMESTAMP_MICROS` |
| I-06 | Datos sucios: fechas fuera del periodo, distancias 0, tarifas negativas, duraciones imposibles | Sesgaría EDA y modelo | 5 reglas de calidad cuantificadas en SILVER + filtro de velocidad < 80 mph |
