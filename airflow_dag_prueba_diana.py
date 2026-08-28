import pendulum

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule


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
# CONFIGURACIÓN ETL
# ============================

ETL_PATH = "/home/python_etl/digercic_etl"
PYTHON = f"{ETL_PATH}/.venv/bin/python"

# Un solo comando - ejecuta ambas tablas en secuencia
CMD_ETL = (
    f"cd {ETL_PATH} && "
    f"{PYTHON} run.py "
    f"--config config/pipeline.yaml"
)


# ============================
# TASKS
# ============================

inicio = DummyOperator(
    task_id="inicio",
    dag=dag,
)


ETL_SYNC = SSHOperator(
    task_id="ETL_SYNC",
    ssh_conn_id="server50",
    command=CMD_ETL,
    dag=dag,
)


fin = DummyOperator(
    task_id="fin",
    dag=dag,
)


# ============================
# SECUENCIA
# ============================

inicio >> ETL_SYNC >> fin
