from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "fraud-transactions"
GROUP_ID = "fraud-spark-group"

def get_kafka_lag():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False
    )

    partitions = consumer.partitions_for_topic(TOPIC)
    if not partitions:
        raise Exception("Topic not found")

    total_lag = 0

    for p in partitions:
        tp = TopicPartition(TOPIC, p)

        consumer.assign([tp])

        end_offset = consumer.end_offsets([tp])[tp]
        committed = consumer.committed(tp)

        if committed is None:
            committed = 0

        total_lag += (end_offset - committed)

    consumer.close()
    return total_lag
