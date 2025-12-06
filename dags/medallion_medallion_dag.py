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


#  Función de limpieza bronze -> clean CSV -> parquet limpio
def _clean_transactions_file(ds_nodash: str, **_context) -> None:
    """
    Limpia el archivo transactions_{ds}.csv o transactions_{ds_nodash}.csv:

    - Busca primero transactions_YYYY-MM-DD.csv
      y luego transactions_YYYYMMDD.csv en data/raw/
    - Elimina filas con amount nulo
    - Escribe el resultado en data/clean/ como parquet:
        transactions_<ds_nodash>_clean.parquet
    """
    # ds_nodash viene como YYYYMMDD → generamos YYYY-MM-DD para posibles archivos
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

    # Nuevo nombre del parquet limpio
    clean_path = CLEAN_DIR / f"transactions_{ds_nodash}_clean.parquet"

    # Leer CSV
    df = pd.read_csv(raw_path)

    if "amount" not in df.columns:
        raise AirflowException("Column 'amount' not found in raw transactions file")

    # Filtramos filas donde amount NO es nulo
    df_clean = df[df["amount"].notna()]

    # Crear carpeta clean si no existe
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    # Guardar parquet
    df_clean.to_parquet(clean_path, index=False)


# --- GOLD / SILVER: función para correr dbt run ---
def _run_dbt_models(ds_nodash: str, **_context) -> None:
    """
    Ejecuta `dbt run` usando ds_nodash para que los modelos
    puedan leer el parquet limpio correcto.

    - Usa _run_dbt_command("run", ds_nodash)
    - Guarda stdout/stderr en:
        data/quality/dbt_run_<ds_nodash>.log
    - Genera un archivo JSON de data quality:
        data/quality/dq_results_<ds_nodash>.json
    - Lanza AirflowException si dbt devuelve código distinto de 0
    """
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    result = _run_dbt_command("run", ds_nodash)

    # Log plano de dbt (por si queremos inspección manual)
    log_path = QUALITY_DIR / f"dbt_run_{ds_nodash}.log"
    log_contents = (
        "Command: dbt run\n"
        f"Return code: {result.returncode}\n\n"
        f"STDOUT:\n{result.stdout}\n\n"
        f"STDERR:\n{result.stderr}\n"
    )
    log_path.write_text(log_contents)

    # JSON de data quality para observabilidad (capa gold)
    dq_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    dq_payload = {
        "ds_nodash": ds_nodash,
        "status": "passed" if result.returncode == 0 else "failed",
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    dq_path.write_text(json.dumps(dq_payload, indent=2))

    if result.returncode != 0:
        raise AirflowException(
            f"dbt run failed for ds_nodash={ds_nodash}. "
            f"See log file: {log_path} and dq file: {dq_path}"
        )


# --- GOLD / OBSERVABILITY: chequear archivo de data quality ---
def _check_data_quality(ds_nodash: str, **_context) -> None:
    """
    Verifica la existencia y el contenido del archivo de data quality:

      data/quality/dq_results_<ds_nodash>.json

    Reglas:
      - El archivo debe existir.
      - Debe ser JSON válido.
      - El campo "status" debe ser "passed".

    Si alguna condición falla → AirflowException.
    """
    dq_path = QUALITY_DIR / f"dq_results_{ds_nodash}.json"

    if not dq_path.exists():
        raise AirflowException(
            f"Data quality file not found: {dq_path}. "
            "Expected after dbt run."
        )

    try:
        dq_data = json.loads(dq_path.read_text())
    except json.JSONDecodeError as exc:
        raise AirflowException(
            f"Data quality file is not valid JSON: {dq_path}"
        ) from exc

    status = dq_data.get("status")
    if status != "passed":
        raise AirflowException(
            f"Data quality check failed for ds_nodash={ds_nodash}. "
            f"Status in {dq_path} is '{status}'."
        )


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

        # Task: correr dbt run usando el parquet limpio (silver/gold)
        run_dbt_models = PythonOperator(
            task_id="run_dbt_models",
            python_callable=_run_dbt_models,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        # Task: observabilidad / data quality (gold)
        check_data_quality = PythonOperator(
            task_id="check_data_quality",
            python_callable=_check_data_quality,
            op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        # Dependencias:
        # Bronze -> Silver/Gold (dbt) -> Gold observabilidad (DQ)
        clean_transactions >> run_dbt_models >> check_data_quality

    return medallion_dag


dag = build_dag()
