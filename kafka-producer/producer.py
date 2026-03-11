import json
import time
import random
from kafka import KafkaProducer

# -------- Producer --------
topic_name = 'player-events'
producer = KafkaProducer(
    bootstrap_servers='192.168.4.32:29092',
    api_version=(3, 5, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

player_ids = [10, 24, 33, 7, 18]

print("Starting producer...")

while True:
    event = {
        "player_id": random.choice(player_ids),
        "timestamp": int(time.time()),
        "x": round(random.uniform(-10, 10), 3),
        "y": round(random.uniform(-10, 10), 3),
        "speed": round(random.uniform(0, 12), 2),
        "confidence": round(random.uniform(0.5, 1.0), 2)
    }

    producer.send(topic_name, event)
    print("Sent:", event)
    time.sleep(1)