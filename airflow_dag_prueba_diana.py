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

CMD_CEDULADOS = (
    f"cd {ETL_PATH} && "
    f"{PYTHON} run.py "
    f"--config config/pipeline_cedulados.yaml"
)

CMD_ATENCIONES = (
    f"cd {ETL_PATH} && "
    f"{PYTHON} run.py "
    f"--config config/pipeline_atenciones.yaml"
)


# ============================
# TASKS
# ============================

inicio = DummyOperator(
    task_id="inicio",
    dag=dag,
)


ETL_CEDULADOS = SSHOperator(
    task_id="ETL_CEDULADOS",
    ssh_conn_id="server50",
    command=CMD_CEDULADOS,
    dag=dag,
)


ETL_ATENCIONES = SSHOperator(
    task_id="ETL_ATENCIONES",
    ssh_conn_id="server50",
    command=CMD_ATENCIONES,
    dag=dag,
)


# Permite continuar aunque ETL_CEDULADOS falle
ETL_ATENCIONES.set_upstream(ETL_CEDULADOS)


fin = DummyOperator(
    task_id="fin",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag,
)


# ============================
# SECUENCIA
# ============================

inicio >> ETL_CEDULADOS >> ETL_ATENCIONES >> fin
