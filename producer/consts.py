DB_DSN = "postgresql://ad_user:ad_pass@localhost:5432/ad_db"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ad_events"

BATCH_SIZE = 100
POLL_INTERVAL = 2
NUM_WORKERS = 4

SQL_STATEMENT = """
    SELECT *
    FROM ad_events
    WHERE id > $1
    ORDER BY id
    LIMIT $2
"""
