from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable, DagModel
from datetime import datetime, timedelta
import psycopg2
import smtplib
from email.mime.text import MIMEText

from kafka_lag_utils import get_kafka_lag

# =====================================================
# CONFIG
# =====================================================
MAX_KAFKA_LAG = 40000
MAX_DB_GAP_SEC = 300  # 5 minutes

RESTART_THRESHOLD = 3      # max restarts allowed
ALERT_EMAILS = ["jindalnimit2016@gmail.com"]

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "jindalnimit2016@gmail.com"
SMTP_PASSWORD = "ytcokwnodmtwzffl"

# =====================================================
# EMAIL UTILS
# =====================================================
def send_email(subject, html):
    msg = MIMEText(html, "html")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(ALERT_EMAILS)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, ALERT_EMAILS, msg.as_string())

# =====================================================
# HEALTH CHECK
# =====================================================
def check_stream_health(**context):
    issues = []

    # ---------- Kafka Lag ----------
    lag = get_kafka_lag()
    if lag > MAX_KAFKA_LAG:
        issues.append(f"Kafka lag too high: {lag}")

    # ---------- Postgres Freshness ----------
    conn = psycopg2.connect(
        dbname="fraud_db",
        user="postgres",
        password="1234",
        host="localhost",
        port=5433,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))
        FROM fraud_transactions
    """)
    gap = cur.fetchone()[0]
    cur.close()
    conn.close()

    if gap is None or gap < 0 or gap > MAX_DB_GAP_SEC:
        issues.append(f"DB freshness gap invalid: {gap}")

    # XCom for email
    ti = context["ti"]
    ti.xcom_push(key="lag", value=lag)
    ti.xcom_push(key="gap", value=gap)

    # Reset restart counter on success
    if not issues:
        Variable.set("fraud_restart_counter", 0)
        return

    raise Exception(" | ".join(issues))

# =====================================================
# EMAIL ALERT
# =====================================================
def send_alert_email(**context):
    lag = context["ti"].xcom_pull(task_ids="check_stream_health", key="lag")
    gap = context["ti"].xcom_pull(task_ids="check_stream_health", key="gap")

    html = f"""
    <h3>🚨 Fraud Streaming Health Alert</h3>
    <p><b>Kafka Lag:</b> {lag}</p>
    <p><b>DB Gap (seconds):</b> {gap}</p>
    <p><b>Action:</b> Restart triggered</p>
    <p><b>Time:</b> {datetime.utcnow()}</p>
    """

    send_email("🚨 Fraud Streaming Alert", html)

# =====================================================
# AUTO DISABLE DAG
# =====================================================
def auto_disable_dag(**context):
    counter = int(Variable.get("fraud_restart_counter", 0)) + 1
    Variable.set("fraud_restart_counter", counter)

    print(f"Restart counter = {counter}")

    if counter > RESTART_THRESHOLD:
        dag_id = context["dag"].dag_id

        # Pause DAG
        DagModel.get_dagmodel(dag_id).set_is_paused(True)

        # 🔴 SEND CRITICAL EMAIL
        send_auto_disable_email(dag_id, counter)

        raise Exception(
            f"DAG {dag_id} auto-disabled after {counter} failed restarts"
        )

    
def send_auto_disable_email(dag_id, counter):
    html = f"""
    <h2 style="color:red;">⛔ FRAUD STREAM AUTO-DISABLED</h2>
    <p><b>DAG:</b> {dag_id}</p>
    <p><b>Reason:</b> Restart failed more than allowed threshold</p>
    <p><b>Restart Attempts:</b> {counter}</p>
    <p><b>Action Required:</b> Manual intervention needed</p>
    <p><b>Time:</b> {datetime.utcnow()}</p>
    """

    send_email(
        subject="⛔ CRITICAL: Fraud Streaming DAG Auto-Disabled",
        html=html
    )


# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="fraud_stream_health_monitor",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    dagrun_timeout=timedelta(minutes=2),
) as dag:

    health_check = PythonOperator(
        task_id="check_stream_health",
        python_callable=check_stream_health,
        provide_context=True,
    )

    email_alert = PythonOperator(
        task_id="send_email_alert",
        python_callable=send_alert_email,
        provide_context=True,
        trigger_rule="one_failed",
    )

    restart_stream = BashOperator(
        task_id="restart_stream",
        trigger_rule="one_failed",
        bash_command="""
        set -e
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        export PATH=$JAVA_HOME/bin:$PATH

        SPARK_PID=$(jps | grep SparkSubmit | awk '{print $1}' || true)
        if [ -n "$SPARK_PID" ]; then
            kill $SPARK_PID
            sleep 10
            kill -9 $SPARK_PID || true
        fi

        spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 \
          --jars /home/dell/jars/postgresql-42.6.0.jar \
          /home/dell/fraud-transactions-streaming/streaming/fraud_stream.py
        """,
    )

    auto_disable = PythonOperator(
        task_id="auto_disable_dag",
        python_callable=auto_disable_dag,
        provide_context=True,
        trigger_rule="one_failed",
    )

    # ✅ CORRECT FLOW
    health_check >> email_alert
    health_check >> restart_stream
    restart_stream >> auto_disable