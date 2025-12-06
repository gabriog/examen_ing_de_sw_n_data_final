## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## **Airflow**

Creamos nuestra primer task en el DAG de Airflow donde generamos la limpieza del archivo .csv en data/raw/.

Luego corremos el DAG medallion_medallion_dag manualmente en localhost para verificar que funcione correctamente. Tambien chequear que se haya generado el .csv limpio en data/clean/.

## **dbt**

Dentro de la ruta dbt/, modificamos inicialmente models/staging/stg_transactions.sql y corremos el modelo en la base de datos. 

```bash
dbt debug
dbt run --vars "{ds: '20251206'}"
```

