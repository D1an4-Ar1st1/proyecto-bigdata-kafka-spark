
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "sensor_data"
BOOTSTRAP_SERVERS = "localhost:9092"


def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

print(f"Enviando datos al topic '{TOPIC}'. Presiona Ctrl+C para detener.")

try:
    while True:
        data = generate_sensor_data()
        producer.send(TOPIC, value=data)
        producer.flush()
        print(f"Enviado: {json.dumps(data, ensure_ascii=False)}")
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()


