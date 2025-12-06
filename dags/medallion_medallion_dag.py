"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

# pylint: disable=import-error,wrong-import-position


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


# --- NUEVO: función de limpieza bronze -> clean CSV ---
def _clean_transactions_file(ds_nodash: str, **_context) -> None:
    """
    Limpia el archivo transactions_{ds}.csv o transactions_{ds_nodash}.csv:

    - Busca primero transactions_YYYY-MM-DD.csv
      y luego transactions_YYYYMMDD.csv en data/raw/
    - Elimina filas con amount nulo
    - Escribe el resultado en data/clean/ con el mismo nombre de archivo
    """
    # ds_nodash viene como YYYYMMDD
    ds = datetime.strptime(ds_nodash, "%Y%m%d").date().isoformat()  # YYYY-MM-DD

    # Probar ambas variantes de nombre
    candidates = [
        RAW_DIR / f"transactions_{ds}.csv",        # transactions_YYYY-MM-DD.csv
        RAW_DIR / f"transactions_{ds_nodash}.csv"  # transactions_YYYYMMDD.csv
    ]

    raw_path = None
    for path in candidates:
        if path.exists():
            raw_path = path
            break

    if raw_path is None:
        tried = ", ".join(str(p) for p in candidates)
        raise AirflowException(f"Raw file not found. Tried: {tried}")

    clean_path = CLEAN_DIR / raw_path.name

    df = pd.read_csv(raw_path)

    if "amount" not in df.columns:
        raise AirflowException("Column 'amount' not found in raw transactions file")

    # Filtramos filas donde amount NO es nulo
    df_clean = df[df["amount"].notna()]

    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(clean_path, index=False)


def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        # Task: limpiar archivo transactions_{ds}.csv (bronze -> clean)
        clean_transactions = PythonOperator(
            task_id="clean_transactions_file",
            python_callable=_clean_transactions_file,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        # TODO (cuando avances):
        # * Agregar las tasks necesarias del pipeline para completar lo pedido por el enunciado.
        # * Usar PythonOperator con el argumento op_kwargs para pasar ds_nodash a las funciones.
        #   De modo que cada task pueda trabajar con la fecha de ejecución correspondiente.
        # Recomendaciones:
        #  * Pasar el argumento ds_nodash a las funciones definidas arriba.
        #    ds_nodash contiene la fecha de ejecución en formato YYYYMMDD sin guiones.
        #    Utilizarlo para que cada task procese los datos del dia correcto y los archivos
        #    de salida tengan nombres únicos por fecha.
        #  * Asegurarse de que los paths usados en las funciones sean relativos a BASE_DIR.
        #  * Usar las funciones definidas arriba para cada etapa del pipeline.

        # Por ahora solo tenemos una task, así que no seteamos dependencias.
        # Más adelante podés hacer:
        # clean_transactions >> otra_task

    return medallion_dag


dag = build_dag()
