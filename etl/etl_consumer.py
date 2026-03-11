import os
import json
from kafka import KafkaConsumer
import psycopg2
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
print("S3_BUCKET from getenv:", os.getenv("S3_BUCKET"))

# ---------- Config ----------
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("KAFKA_TOPIC")

PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

# ---------- Setup Postgres ----------
def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def create_table():
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_events (
            id SERIAL PRIMARY KEY,
            player_id INT,
            ts BIGINT,
            x DOUBLE PRECISION,
            y DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            confidence DOUBLE PRECISION,
            created_at TIMESTAMP DEFAULT now()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# ---------- Setup MinIO ----------
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
)

# Create bucket if not exists
try:
    s3.head_bucket(Bucket=S3_BUCKET)
except:
    s3.create_bucket(Bucket=S3_BUCKET)

# ---------- Kafka Consumer ----------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="etl-group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    api_version=(3, 5, 0),
)

create_table()

print("ETL consumer started...")

for message in consumer:
    event = message.value

    # ---- Store raw in MinIO ----
    key = f"raw/{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(event).encode("utf-8")
    )

    # ---- Transform ----
    transformed = (
        event["player_id"],
        event["timestamp"],
        event["x"],
        event["y"],
        event["speed"],
        event["confidence"],
    )

    # ---- Insert into Postgres ----
    conn = get_pg_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO player_events (player_id, ts, x, y, speed, confidence)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, transformed)
    conn.commit()
    cur.close()
    conn.close()

    print("Processed event:", event)