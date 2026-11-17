"""Етап 2. Імітація одного датчика.

Один запуск скрипту = один датчик. Щоб імітувати декілька датчиків,
запустіть скрипт у декількох терміналах.
"""

import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import TOPIC_SENSORS, JsonSerializer, KeySerializer, connection_kwargs

SEND_INTERVAL_SECONDS = 2


def main() -> None:
    producer = KafkaProducer(
        **connection_kwargs(),
        value_serializer=JsonSerializer(),
        key_serializer=KeySerializer(),
    )

    # ID датчика — випадковий, але постійний протягом одного запуску
    sensor_id = random.randint(1000, 9999)
    print(f"Sensor {sensor_id} started. Sending to '{TOPIC_SENSORS}'. Ctrl+C to stop.")

    try:
        while True:
            data = {
                "sensor_id": sensor_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "temperature": random.randint(25, 45),
                "humidity": random.randint(15, 85),
            }
            producer.send(TOPIC_SENSORS, key=sensor_id, value=data)
            producer.flush()
            print(f"Sent to '{TOPIC_SENSORS}': {data}")
            time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"\nSensor {sensor_id} stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
