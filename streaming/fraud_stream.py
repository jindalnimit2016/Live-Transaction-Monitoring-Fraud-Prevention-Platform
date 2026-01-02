from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import psycopg2

# ---------------------------------------
# Spark
# ---------------------------------------
spark = (
    SparkSession.builder
    .appName("FraudDetectionStream-Stable")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------
# Kafka schema
# ---------------------------------------
schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("device_type", StringType()),
    StructField("ip_address", StringType()),
    StructField("payment_method", StringType()),
    StructField("timestamp", StringType())
])

# ---------------------------------------
# Kafka stream
# ---------------------------------------
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "fraud_transactions")
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), schema).alias("d"))
    .select("d.*")
    .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
)

# ---------------------------------------
# Postgres config
# ---------------------------------------
jdbc_url = "jdbc:postgresql://localhost:5433/fraud_db"
jdbc_props = {
    "user": "postgres",
    "password": "1234",
    "driver": "org.postgresql.Driver"
}

pg_conn_params = {
    "host": "localhost",
    "port": 5433,
    "dbname": "fraud_db",
    "user": "postgres",
    "password": "1234"
}

# ---------------------------------------
# Load profiles
# ---------------------------------------
def load_profiles():
    users = spark.read.jdbc(jdbc_url, "user_profiles", properties=jdbc_props)
    merchants = spark.read.jdbc(jdbc_url, "merchant_profiles", properties=jdbc_props)
    return users, merchants

# ---------------------------------------
# Postgres UPSERT (REAL FIX)
# ---------------------------------------
def upsert_profiles_postgres():

    conn = psycopg2.connect(**pg_conn_params)
    cur = conn.cursor()

    # ---- USER PROFILES ----
    cur.execute("""
        INSERT INTO user_profiles (
            user_id, home_country, avg_amount, std_amount,
            normal_devices, risk_score, last_transaction
        )
        SELECT
            user_id,
            MAX(home_country) AS home_country,
            AVG(avg_amount) AS avg_amount,
            AVG(std_amount) AS std_amount,
            ARRAY_AGG(DISTINCT device) AS normal_devices,
            AVG(risk_score) AS risk_score,
            MAX(last_transaction) AS last_transaction
        FROM (
            SELECT
                user_id,
                home_country,
                avg_amount,
                std_amount,
                unnest(normal_devices) AS device,
                risk_score,
                last_transaction
            FROM user_profiles_staging
        ) t
        GROUP BY user_id
        ON CONFLICT ON CONSTRAINT user_profiles_pk
        DO UPDATE SET
            home_country = EXCLUDED.home_country,
            avg_amount = EXCLUDED.avg_amount,
            std_amount = EXCLUDED.std_amount,
            normal_devices = EXCLUDED.normal_devices,
            risk_score = EXCLUDED.risk_score,
            last_transaction = EXCLUDED.last_transaction;
    """)


    # ---- MERCHANT PROFILES ----
    cur.execute("""
        INSERT INTO merchant_profiles (
            merchant_id, category, risk_score, avg_amount, txn_count
        )
        SELECT
            merchant_id,
            MAX(category) AS category,
            AVG(risk_score) AS risk_score,
            AVG(avg_amount) AS avg_amount,
            SUM(txn_count) AS txn_count
        FROM merchant_profiles_staging
        GROUP BY merchant_id
        ON CONFLICT ON CONSTRAINT merchant_profiles_pk
        DO UPDATE SET
            category = EXCLUDED.category,
            risk_score = EXCLUDED.risk_score,
            avg_amount = EXCLUDED.avg_amount,
            txn_count = merchant_profiles.txn_count + EXCLUDED.txn_count;
    """)


    conn.commit()
    cur.close()
    conn.close()

# ---------------------------------------
# Batch processing
# ---------------------------------------
def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    users, merchants = load_profiles()

    df = (
        batch_df
        .withColumn("merchant", col("merchant_id"))
        .withColumn("location", concat_ws(",", col("country"), col("city")))
    )

    # Join profiles
    df = (
        df.join(users.select("user_id", "risk_score"), "user_id", "left")
          .withColumnRenamed("risk_score", "risk_score_user")
          .join(merchants.select("merchant_id", "risk_score"), "merchant_id", "left")
          .withColumnRenamed("risk_score", "risk_score_merchant")
          .fillna({"risk_score_user": 0.1, "risk_score_merchant": 0.1})
    )

    # Rules
    df = df.withColumn("fraud_high_amount", when(col("amount") > 1200, 1).otherwise(0))

    w = Window.partitionBy("user_id")
    df = df.withColumn("fraud_rapid_txn", when(count("*").over(w) > 3, 1).otherwise(0))

    df = df.withColumn(
        "is_fraud",
        ((col("fraud_high_amount") == 1) | (col("fraud_rapid_txn") == 1)).cast("int")
    )

    # Transactions
    df.select(
        "transaction_id", "user_id", "amount", "currency",
        "merchant", "location", "is_fraud", "timestamp",
        "fraud_high_amount", "fraud_rapid_txn",
        "merchant_id", "country", "city",
        "device_type", "ip_address", "payment_method"
    ).write.jdbc(jdbc_url, "fraud_transactions", "append", jdbc_props)

    # Alerts
    alerts = (
        df.filter(col("is_fraud") == 1)
          .withColumn("fraud_type",
              when((col("fraud_high_amount") == 1) & (col("fraud_rapid_txn") == 1),
                   "HIGH_AMOUNT + RAPID_TXN")
              .when(col("fraud_rapid_txn") == 1, "RAPID_TXN")
              .otherwise("HIGH_AMOUNT"))
          .withColumn("severity", when(col("fraud_rapid_txn") == 1, "HIGH").otherwise("MEDIUM"))
          .withColumn("model_score", lit(0.0))
    )

    alerts.select(
        "transaction_id", "user_id", "merchant_id",
        "fraud_type", "severity", "amount",
        col("timestamp").alias("created_at"),
        "country", "city", "device_type",
        "model_score", "payment_method"
    ).write.jdbc(jdbc_url, "fraud_alerts", "append", jdbc_props)

    # -------- PROFILE UPDATES --------
    user_updates = (
        df.groupBy("user_id")
          .agg(
                first("country").alias("home_country"),
                avg("amount").alias("avg_amount"),
                stddev("amount").alias("std_amount"),
                collect_set("device_type").alias("normal_devices"),
                max("timestamp").alias("last_transaction")
            )
          .withColumnRenamed("country", "home_country")
          .withColumn("risk_score", lit(0.5))
    )

    user_updates.write.jdbc(jdbc_url, "user_profiles_staging", "overwrite", jdbc_props)

    merchant_updates = (
        df.groupBy("merchant_id")
          .agg(avg("amount").alias("avg_amount"), count("*").alias("txn_count"))
          .withColumn("category", lit("UNKNOWN"))
          .withColumn("risk_score", lit(0.5))
    )

    merchant_updates.write.jdbc(jdbc_url, "merchant_profiles_staging", "overwrite", jdbc_props)

    # REAL UPSERT
    upsert_profiles_postgres()

# ---------------------------------------
# Start stream
# ---------------------------------------
query = (
    stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/tmp/fraud_checkpoint_stable")
    .start()
)

query.awaitTermination(200)
query.stop()
spark.stop()
