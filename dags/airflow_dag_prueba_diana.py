import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


# ============================================================
# CONFIGURACION GENERAL
# ============================================================

DAG_ID = "airflow_dag_prueba_diana"
SSH_CONNECTION = "server50"
PROJECT_PATH = "/home/python_etl/digercic_etl"
PYTHON_PATH = f"{PROJECT_PATH}/.venv/bin/python"
CONFIG_PATH = "config/pipeline.yaml"

def on_failure_callback(context):
    ti = context.get('task_instance')
    print(f"TASK FAILED: {ti.task_id} - Dag: {ti.dag_id} - Exec: {context.get('execution_date')}")

def on_success_callback(context):
    ti = context.get('task_instance')
    print(f"TASK SUCCESS: {ti.task_id}")

default_args = {
    "owner": "DSN",
    "retries": 4,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": on_failure_callback,
}


# ============================================================
# COMANDOS - Todo el output va a stdout para log de Airflow (142)
# ============================================================

COMMAND_CHECK = f"""
    set -e
    cd {PROJECT_PATH}
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [AIRFLOW] [1/2] Verificando conexiones..."
    {PYTHON_PATH} scripts/check_connections.py --config {CONFIG_PATH} 2>&1
    echo "[AIRFLOW] Check conexiones OK"
"""

COMMAND_ETL = f"""
    set -e
    cd {PROJECT_PATH}
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [AIRFLOW] [2/2] Iniciando ETL en $(hostname)..."
    {PYTHON_PATH} run.py --config {CONFIG_PATH} 2>&1
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
      echo "[AIRFLOW] ETL fallo con exit code $EXIT_CODE" >&2
      exit $EXIT_CODE
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [AIRFLOW] ETL finalizado correctamente."
 """


# ============================================================
# DEFINICION DEL DAG
# ============================================================

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 31, tz="America/Guayaquil"),
    schedule_interval="30 11 * * *",
    catchup=False,
) as dag:

    inicio = DummyOperator(task_id="inicio")

    # Tarea 1: Verifica Oracle y Postgres ANTES de lanzar ETL
    # Si falla, el DAG se marca en ROJO aquí y no ejecuta ETL
    check_connections = SSHOperator(
        task_id="check_connections",
        ssh_conn_id=SSH_CONNECTION,
        command=COMMAND_CHECK,
        conn_timeout=30,
        cmd_timeout=120,
        keepalive_interval=10,
    )

    etl_capta = SSHOperator(
        task_id="ETL_CAPTA",
        ssh_conn_id=SSH_CONNECTION,
        command=COMMAND_ETL,
        conn_timeout=30,
        cmd_timeout=14400,  # 4 horas max para tablas grandes
        keepalive_interval=30,
        on_success_callback=on_success_callback,
    )

    fin = DummyOperator(task_id="fin", trigger_rule="all_success")

    inicio >> check_connections >> etl_capta >> fin
