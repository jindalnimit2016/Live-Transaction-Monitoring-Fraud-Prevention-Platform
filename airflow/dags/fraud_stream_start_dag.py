from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="fraud_stream_start",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # MANUAL ONLY
    catchup=False,
) as dag:

    start_stream = BashOperator(
        task_id="start_spark_stream",
        bash_command="""
        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
          --jars /home/dell/jars/postgresql-42.6.0.jar \
          --conf spark.dynamicAllocation.enabled=true \
          --conf spark.dynamicAllocation.minExecutors=1 \
          --conf spark.dynamicAllocation.maxExecutors=4 \
          /home/dell/fraud-transactions-streaming/streaming/fraud_stream.py 
        """
    )
