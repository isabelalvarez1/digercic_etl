import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


default_args = {
    "owner": "DSN",
    "retries": 4,
}


with DAG(
    dag_id="airflow_dag_prueba_diana",
    default_args=default_args,

    start_date=pendulum.datetime(
        2026,
        8,
        31,
        tz="America/Guayaquil",
    ),

    schedule_interval="30 11 * * *",

    catchup=False,

) as dag:

    inicio = DummyOperator(
        task_id="inicio",
    )

    etl_capta = SSHOperator(
        task_id="ETL_CAPTA",
        ssh_conn_id="server50",

        command="""
            cd /home/python_etl/digercic_etl &&
            .venv/bin/python run.py \
            --config config/pipeline.yaml
        """,
    )

    fin = DummyOperator(
        task_id="fin",
    )

    inicio >> etl_capta >> fin
