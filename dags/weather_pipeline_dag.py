from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# 1. Configuración de parámetros por defecto (Mantenimiento)
default_args = {
    'owner': 'forne',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1), # Año actual 2026
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Definición del DAG
with DAG(
    'pipeline_clima_medallion',
    default_args=default_args,
    description='Pipeline completo de OpenWeather: Ingesta -> Plata -> Oro',
    schedule_interval='@hourly', # ⏱️ Se ejecutará automáticamente cada hora
    catchup=False,
    tags=['weather', 'spark', 'kafka'],
) as dag:

    PROYECTO_DIR = "/opt/airflow/project"

    t1_ingest_api_to_kafka = BashOperator(
        task_id='ingestar_api_a_kafka',
        bash_command=f"python {PROYECTO_DIR}/ingesting.py",
    )

    t2_bronze_to_silver = BashOperator(
        task_id='transformar_bronze_a_silver',
        bash_command=f"python {PROYECTO_DIR}/bronze_to_silver.py",
    )

    t3_silver_to_gold = BashOperator(
        task_id='consolidar_silver_a_gold',
        bash_command=f"python {PROYECTO_DIR}/silver_to_gold.py",
    )

    t1_ingest_api_to_kafka >> t2_bronze_to_silver >> t3_silver_to_gold