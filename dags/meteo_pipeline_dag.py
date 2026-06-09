from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from meteo_pipeline import (
    recuperer_meteo,
    transformer_meteo,
    charger_postgresql,
    enregistrer_suivi
)


def extract(**context):
    data = recuperer_meteo()
    context["ti"].xcom_push(key="meteo", value=data)


def transform(**context):
    raw = context["ti"].xcom_pull(
        task_ids="extract",
        key="meteo"
    )

    clean = transformer_meteo(raw)

    context["ti"].xcom_push(
        key="clean_data",
        value=clean
    )


def load(**context):
    data = context["ti"].xcom_pull(
        task_ids="transform",
        key="clean_data"
    )

    charger_postgresql(data)


with DAG(
    dag_id="meteo_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load
    )

    audit_task = PythonOperator(
        task_id="audit",
        python_callable=enregistrer_suivi
    )

    extract_task >> transform_task >> load_task >> audit_task
