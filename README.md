# Medallion Architecture Demo (Airflow + dbt + DuckDB)

Este proyecto crea un pipeline de 3 pasos que replica la arquitectura medallion:

1. **Bronze**: Airflow lee un CSV crudo según la fecha de ejecución y aplica una limpieza básica con Pandas guardando un archivo parquet limpio.
2. **Silver**: Un `dbt run` carga el parquet en DuckDB y genera modelos intermedios.
3. **Gold**: `dbt test` valida la tabla final y escribe un archivo con el resultado de los data quality checks.

## Estructura

```
├── dags/
│   └── medallion_medallion_dag.py
├── data/
│   ├── raw/
│   │   └── transactions_20251205.csv
│   ├── clean/
│   └── quality/
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
├── include/
│   └── transformations.py
├── profiles/
│   └── profiles.yml
├── warehouse/
│   └── medallion.duckdb (se genera en tiempo de ejecución)
└── requirements.txt
```

## Requisitos

- Python 3.10+
- DuckDB CLI opcional para inspeccionar la base.

Instala dependencias:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuración de variables de entorno

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

## Inicializar Airflow

```bash
airflow standalone
```

En el output de `airflow standalone` buscar la contraseña para loguearse. Ej:

```
standalone | Airflow is ready
standalone | Login with username: admin  password: pPr9XXxFzgrgGd6U
```

## Ejecutar el DAG

1. Coloca/actualiza el archivo `data/raw/transactions_YYYYMMDD.csv`.

3. Desde la UI o CLI dispara el DAG usando la fecha deseada:

```bash
airflow dags trigger medallion_pipeline --run-id manual_$(date +%s)
```

El DAG ejecutará:

- `bronze_clean`: llama a `clean_daily_transactions` para crear `data/clean/transactions_<ds>_clean.parquet`.
- `silver_dbt_run`: ejecuta `dbt run` apuntando al proyecto `dbt/` y carga la data en DuckDB.
- `gold_dbt_tests`: corre `dbt test` y escribe `data/quality/dq_results_<ds>.json` con el status (`passed`/`failed`).

Si un test falla, el archivo igual se genera y el task termina en error para facilitar el monitoreo.

## Ejecutar dbt manualmente

```bash
cd dbt
dbt run
DBT_PROFILES_DIR=../profiles dbt test
```

Asegúrate de exportar `CLEAN_DIR`, `DS_NODASH` y `DUCKDB_PATH` si necesitas sobreescribir valores por defecto:

```bash
export CLEAN_DIR=$(pwd)/../data/clean
export DS_NODASH=20251205
export DUCKDB_PATH=$(pwd)/../warehouse/medallion.duckdb
```

## Observabilidad de Data Quality

Cada corrida crea `data/quality/dq_results_<ds>.json` similar a:

```json
{
  "ds_nodash": "20251205",
  "status": "passed",
  "stdout": "...",
  "stderr": ""
}
```

Ese archivo puede ser ingerido por otras herramientas para auditoría o alertas.

## Verificación de resultados por capa

### Bronze

1. Revisa que exista el parquet más reciente:

    ```bash
    $ find data/clean/ | grep transactions_*
    data/clean/transactions_20251201_clean.parquet
    ```

2. Inspecciona las primeras filas para confirmar la limpieza aplicada:

    ```bash
    duckdb -c "
      SELECT *
      FROM read_parquet('data/clean/transactions_20251201_clean.parquet')
      LIMIT 5;
    "
    ```

### Silver

1. Abre el warehouse y lista las tablas creadas por dbt:

    ```bash
    duckdb warehouse/medallion.duckdb -c ".tables"
    ```

2. Ejecuta consultas puntuales para validar cálculos intermedios:

    ```bash
    duckdb warehouse/medallion.duckdb -c "
      SELECT *
      FROM fct_customer_transactions
      LIMIT 10;
    "
    ```

### Gold

1. Revisa que exista el parquet más reciente:

    ```bash
    $ find data/quality/*.json
    data/quality/dq_results_20251201.json
    ```

2. Confirma la generación del archivo de data quality:

    ```bash
    cat data/quality/dq_results_20251201.json | jq
    ```

3. En caso de fallos, inspecciona `stderr` dentro del mismo JSON o revisa los logs del task en la UI/CLI de Airflow para identificar la prueba que reportó error.

## Formato y linting

Usa las herramientas incluidas en `requirements.txt` para mantener un estilo consistente y detectar problemas antes de ejecutar el DAG.

### Black (formateo)

Aplica Black sobre los módulos de Python del proyecto. Añade rutas extra si incorporas nuevos paquetes.

```bash
black dags include
```

### isort (orden de imports)

Ordena automáticamente los imports para evitar diffs innecesarios y mantener un estilo coherente.

```bash
isort dags include
```

### Pylint (estático)

Ejecuta Pylint sobre las mismas carpetas para detectar errores comunes y mejorar la calidad del código.

```bash
pylint dags/*.py include/*.py
```

Para ejecutar ambos comandos de una vez puedes usar:

```bash
isort dags include && black dags include && pylint dags/*.py include/*.py
```

## TODOs

Necesarios para completar el workflow:

- [ ] Implementar tareas de Airflow.
- [ ] Implementar modelos de dbt según cada archivo schema.yml.
- [ ] Implementar pruebas de dbt para asegurar que las tablas gold estén correctas.
- [ ] Documentar mejoras posibles para el proceso considerado aspectos de escalabilidad y modelado de datos.

Nice to have:

- [ ] Manejar el caso que no haya archivos para el dia indicado.

---

## Resolución de TODOs

### ✅ Implementar tareas de Airflow

El DAG `medallion_pipeline` fue implementado utilizando 4 tareas que procesan datos de forma secuencial:

1. **`check_raw_file_exists`**: Verifica la existencia del archivo raw antes de procesar. Si no existe, usa `AirflowSkipException` para saltear el pipeline completo de forma graceful, evitando fallos innecesarios cuando no hay datos para procesar.

2. **`clean_transactions_file`**: Realiza limpieza básica con Pandas, eliminando duplicados, normalizando columnas y filtrando registros inválidos. El resultado se guarda como parquet en `data/clean/`.

3. **`run_dbt_models`**: Ejecuta `dbt run` para generar las capas Silver (staging) y Gold (marts) en DuckDB. Las variables de entorno (`CLEAN_DIR`, `DS_NODASH`, `DUCKDB_PATH`) permiten que dbt procese el archivo correcto para cada fecha de ejecución.

4. **`check_data_quality`**: Valida que los tests de dbt hayan pasado y genera un archivo JSON con los resultados en `data/quality/`, facilitando la observabilidad y auditoría del pipeline.

Todas las tareas downstream usan `trigger_rule="none_failed_min_one_success"` para manejar correctamente los casos donde la primera tarea se saltea por falta de datos.

### ✅ Implementar modelos de dbt según cada archivo schema.yml

Se crearon dos modelos dbt que procesan los datos en capas:

**Staging (`stg_transactions`)**:

- Lee el archivo parquet limpio que generó Pandas
- Convierte los tipos de datos para que sean consistentes (ej: `amount` como número decimal)
- Calcula `transaction_date` a partir del timestamp para facilitar análisis por día
- Se guarda como `view` (no ocupa espacio extra, siempre muestra datos actuales)

**Marts (`fct_customer_transactions`)**:

- Toma los datos de staging y los agrupa por cliente
- Calcula métricas útiles: cantidad de transacciones, monto total completado, monto total general
- Se guarda como `table` (ocupa espacio pero las consultas son más rápidas)
- Permite analizar el comportamiento de cada cliente de forma eficiente

Esta separación en dos capas hace que sea más fácil mantener el código y agregar nuevas fuentes de datos en el futuro.

### ✅ Implementar pruebas de dbt para asegurar que las tablas gold estén correctas

Se implementaron tests de calidad de datos en dos niveles:

**Tests genéricos personalizados** (`dbt/tests/generic/non_negative.sql`):

1. **`non_negative`**: Valida que valores numéricos no sean negativos ni nulos
2. **`not_future_date`**: Verifica que fechas no sean mayores al día actual
3. **`not_empty_string`**: Detecta strings vacíos o con solo espacios

**Cobertura de tests**:

- **Staging**: Validación estricta en el punto de entrada (uniqueness, not_null, non_negative, accepted_values, not_future_date, not_empty_string)
- **Marts**: Tests básicos sobre métricas derivadas (not_null, non_negative)

### ✅ Documentar mejoras posibles para el proceso considerando aspectos de escalabilidad y modelado de datos

**1. Procesar más datos**:

- Actualmente Pandas carga todo en memoria. Para archivos más grandes, se podría usar Polars (más eficiente) o Spark (para datos distribuidos).
- DuckDB funciona bien hasta ~100GB. Para más, considerar bases de datos cloud como Snowflake o BigQuery.

**2. Ejecutar más seguido**:

- Ahora el DAG corre una vez al día. Se podría cambiar a cada hora o usar streaming (Kafka) para procesar datos en tiempo real.
- En dbt, usar modelos incrementales para solo procesar datos nuevos en lugar de todo desde cero.

**3. Mejor monitoreo**:

- Enviar métricas a Grafana para visualizar el estado del pipeline en dashboards.
- Configurar alertas en Slack cuando fallen los tests de calidad.

### ✅ Manejar el caso que no haya archivos para el día indicado

Se implementó utilizando las capacidades nativas de Airflow:

La tarea `check_raw_file_exists` verifica la existencia del archivo raw antes de iniciar el procesamiento. Si el archivo no existe, lanza `AirflowSkipException` con un mensaje informativo indicando qué archivos se buscaron. Esto marca la tarea como "skipped" en lugar de "failed".

Las tareas downstream (`clean_transactions_file`, `run_dbt_models`, `check_data_quality`) están configuradas con `trigger_rule="none_failed_min_one_success"`, lo que les permite saltearse automáticamente cuando la tarea anterior se saltea.

---

## Aplicación de Formato y Linting

El proyecto incluye herramientas de calidad de código en `requirements.txt` que ayudan a mantener un estilo consistente y detectar problemas antes de ejecutar el DAG.

### Herramientas Disponibles

**Black** - Formateador automático de código Python

- Aplica el estilo PEP 8 de forma automática
- No requiere configuración, es opinionado por diseño
- Reformatea el código para que sea consistente en todo el proyecto

**isort** - Ordenador de imports

- Organiza los imports en grupos (stdlib, third-party, local)
- Los ordena alfabéticamente dentro de cada grupo
- Elimina imports duplicados

**Pylint** - Analizador estático de código

- Detecta errores potenciales antes de ejecutar el código
- Identifica code smells y problemas de estilo
- Da una puntuación de 0-10 para medir la calidad del código

### Cómo Usar las Herramientas

**Formatear código con Black:**

```bash
black dags include
```

**Ordenar imports con isort:**

```bash
isort dags include
```

**Analizar código con Pylint:**

```bash
pylint dags/*.py include/*.py
```

**Ejecutar todo de una vez:**

```bash
isort dags include && black dags include && pylint dags/*.py include/*.py
```

### Ejemplo de Salida

```sh
$ isort dags include && black dags include && pylint dags/*.py include/*.py
reformatted /home/gabo/repos/examen_ing_de_sw_n_data_final/dags/medallion_medallion_dag.py

All done! ✨ 🍰 ✨
1 file reformatted, 1 file left unchanged.
************* Module medallion_medallion_dag
dags/medallion_medallion_dag.py:278:8: W0104: Statement seems to have no effect (pointless-statement)
dags/medallion_medallion_dag.py:25:0: W0611: Unused clean_daily_transactions imported from include.transformations (unused-import)

------------------------------------------------------------------
Your code has been rated at 9.83/10 (previous run: 9.83/10, +0.00)

$
```

### Cuándo Aplicar

Se recomienda ejecutar estas herramientas:

- Antes de hacer commit de cambios
- Después de agregar nuevas funciones o tareas
- Como parte del proceso de code review
- Antes de hacer merge a la rama principal
