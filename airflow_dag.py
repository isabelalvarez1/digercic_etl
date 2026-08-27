import airflow
from datetime import datetime, timedelta
import pendulum
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule

args = {
    'owner': 'DSN',
    'start_date': pendulum.datetime(2022, 8, 8, tz='America/Guayaquil'),
    'retries': 4,
}

dag = DAG(
    dag_id='capta_sync_rraa_rree_2',
    default_args=args,
    schedule_interval="30 16 * * 1,3",
    catchup=False,
)

# ============================
# COMANDO ETL SERVIDOR 24
# ============================
ETL_PATH = "/path/to/digercic_etl"
PYTHON = f"{ETL_PATH}/.venv/bin/python"

# Un solo comando que ejecuta ambas extracciones y cargas
CMD_ETL_COMPLETO = f"cd {ETL_PATH} && {PYTHON} run.py --config config/pipeline.yaml"

# ============================
# TASKS
# ============================

inicio = DummyOperator(task_id="inicio", dag=dag)

# --- Cedulados MSP ---
RC_101_sync = SSHOperator(
    task_id="RC_101_sync",
    ssh_conn_id="ssh_server_24",
    command=CMD_ETL_COMPLETO,
    dag=dag,
)

RC_101_ratore = DummyOperator(task_id="RC_101_ratore", dag=dag)

# --- MSP 212 ---
MSP_212 = DummyOperator(task_id="MSP_212", dag=dag)
MSP_212_sync = SSHOperator(
    task_id="MSP_212_sync",
    ssh_conn_id="ssh_server_24",
    command=CMD_ETL_COMPLETO,
    dag=dag,
)
MSP_212_ratore = DummyOperator(task_id="MSP_212_ratore", dag=dag)

# --- MSP 213 ---
MSP_213 = DummyOperator(task_id="MSP_213", dag=dag)
MSP_213_sync = SSHOperator(
    task_id="MSP_213_sync",
    ssh_conn_id="ssh_server_24",
    command=CMD_ETL_COMPLETO,
    dag=dag,
)
MSP_213_ratore = DummyOperator(task_id="MSP_213_ratore", dag=dag)

# --- Permisos y flujo posterior ---
Permiso00 = DummyOperator(task_id="Permiso00", dag=dag)
RC_102_precision = DummyOperator(task_id="RC_102_precision", dag=dag)
REGISTRO_POBLACION = DummyOperator(task_id="REGISTRO_POBLACION", dag=dag)
MSP_213_precision = DummyOperator(task_id="MSP_213_precision", dag=dag)
MSP_212_precision = DummyOperator(task_id="MSP_212_precision", dag=dag)
Permiso01 = DummyOperator(task_id="Permiso01", dag=dag)
REGISTRO_PADRON = DummyOperator(task_id="REGISTRO_PADRON", dag=dag)
padron_frontera = DummyOperator(task_id="padron_frontera", dag=dag)
PADRON_HISTORICO = DummyOperator(task_id="PADRON_HISTORICO", dag=dag)
INTEGRACION = DummyOperator(task_id="INTEGRACION", dag=dag)
Permiso02 = DummyOperator(task_id="Permiso02", dag=dag)
REGISTRO_ALERTAS = DummyOperator(task_id="REGISTRO_ALERTAS", dag=dag)
Permiso03 = DummyOperator(task_id="Permiso03", dag=dag)
REGISTRO_INDIVIDUAL = DummyOperator(task_id="REGISTRO_INDIVIDUAL", dag=dag)
fin = DummyOperator(task_id="fin", dag=dag)

# ============================
# SECUENCIA
# ============================

inicio >> [RC_101_sync, MSP_212]

RC_101_sync >> RC_101_ratore

MSP_212 >> MSP_212_sync >> MSP_212_ratore >> MSP_213 >> MSP_213_sync >> MSP_213_ratore

[RC_101_ratore, MSP_213_ratore] >> Permiso00

[Permiso00 >> RC_102_precision >> REGISTRO_POBLACION >> MSP_213_precision >> MSP_212_precision >> Permiso01 >> REGISTRO_PADRON >> padron_frontera >> PADRON_HISTORICO >> INTEGRACION >> Permiso02 >> REGISTRO_ALERTAS >> Permiso03 >> REGISTRO_INDIVIDUAL] >> fin
