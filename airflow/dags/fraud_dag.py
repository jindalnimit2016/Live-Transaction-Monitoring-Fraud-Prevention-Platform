from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# ---------------------------
# Postgres Validation
# ---------------------------
def check_postgres_data():
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="fraud_db",
        user="postgres",
        password="1234"
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM fraud_transactions")
    count = cur.fetchone()[0]

    if count == 0:
        raise ValueError("No data in fraud_transactions table")

    print(f"Rows in fraud_transactions: {count}")

    cur.close()
    conn.close()

# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id="fraud_kafka_spark_postgres_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    tags=["kafka", "spark", "postgres"]
) as dag:

    start_spark_stream = BashOperator(
        task_id="start_spark_stream",
        bash_command="""
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
          --jars /home/dell/jars/postgresql-42.6.0.jar \
          /home/dell/fraud-transactions-streaming/streaming/fraud_stream.py
        """,
        execution_timeout=timedelta(minutes=6),
        dag=dag,
    )

    validate_postgres = PythonOperator(
        task_id="validate_postgres_data",
        python_callable=check_postgres_data
    )

    start_spark_stream >> validate_postgres
