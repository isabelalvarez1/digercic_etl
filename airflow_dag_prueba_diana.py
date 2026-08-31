import pendulum

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook


args = {
    "owner": "DSN",
    "start_date": pendulum.datetime(
        2022, 8, 8,
        tz="America/Guayaquil",
    ),
    "retries": 4,
}


dag = DAG(
    dag_id="airflow_dag_prueba_diana",
    default_args=args,
    schedule_interval="30 11 * * *",
    catchup=False,
)


# ============================
# CONFIGURACION ETL
# ============================

ETL_PATH = "/home/python_etl/digercic_etl"
PYTHON = f"{ETL_PATH}/.venv/bin/python"
CONFIG = f"{ETL_PATH}/config/pipeline.yaml"


def cmd_etl(table_name=None):
    """Genera el comando ETL para una tabla especifica o todas."""
    cmd = f"cd {ETL_PATH} && {PYTHON} run.py --config {CONFIG}"
    if table_name:
        cmd += f" --table {table_name}"
    return cmd


# ============================
# CONEXION SSH
# ============================

ssh_hook = SSHHook(ssh_conn_id="server50")


# ============================
# TASKS
# ============================

inicio = DummyOperator(
    task_id="inicio",
    dag=dag,
)

# --- Tabla 1: cedulados_msp ---
cedulados_extract = SSHOperator(
    task_id="cedulados_msp_extract_load",
    ssh_hook=ssh_hook,
    command=cmd_etl("cedulados_msp"),
    dag=dag,
)

# --- Tabla 2: atenciones_ninos_hcue ---
atenciones_extract = SSHOperator(
    task_id="atenciones_ninos_hcue_extract_load",
    ssh_hook=ssh_hook,
    command=cmd_etl("atenciones_ninos_hcue"),
    dag=dag,
)

fin = DummyOperator(
    task_id="fin",
    dag=dag,
)


# ============================
# SECUENCIA
# ============================

inicio >> [cedulados_extract, atenciones_extract] >> fin
