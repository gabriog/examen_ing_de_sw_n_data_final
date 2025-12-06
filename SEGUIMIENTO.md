## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Desde la carpeta principal:
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Exportamos CLEAN_DIR, DS_NODASH y DUCKDB_PATH si necesitas sobreescribir valores por defecto:
```bash
export CLEAN_DIR=$(pwd)/data/clean
export DS_NODASH=20251206
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
```

## **Airflow**

Creamos nuestra primer task en el DAG de Airflow donde generamos la limpieza del archivo .csv en data/raw/.

Luego corremos el DAG medallion_medallion_dag manualmente en localhost para verificar que funcione correctamente. Tambien chequear que se haya generado el .csv limpio en data/clean/.

## **dbt**

Dentro de la ruta dbt/, modificamos inicialmente models/staging/stg_transactions.sql y corremos el modelo en la base de datos. 

Generalizamos el PATH para CLEAN en dbt_project.yml:
```bash
clean_dir: "{{ env_var('CLEAN_DIR', project_root ~ '/../data/clean') }}"
```

Probamos que el modelo funcione correctamente
```bash
dbt debug
dbt run --vars "{ds: '20251206'}"
```

Verificamos que los datos se encuentren en el warehouse con una consulta SQL:

``` bash
duckcli warehouse/medallion.duckdb
SELECT * FROM ventas_clean;
```
