from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from configs import (
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    TEMPERATURE_MAX,
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_NAME_SPACE,
    TOPIC_SENSORS,
    TOPIC_TEMPERATURE_ALERTS,
    JsonDeserializer,
    JsonSerializer,
    KeySerializer,
    connection_kwargs,
)


def build_alert(data: dict, message: str) -> dict:
    return {
        "sensor_id": data["sensor_id"],
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "timestamp": data["timestamp"],
        "alert_time": datetime.now(timezone.utc).isoformat(),
        "message": message,
    }


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_SENSORS,
        **connection_kwargs(),
        value_deserializer=JsonDeserializer(),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{TOPIC_NAME_SPACE}_alert_processor",
    )
    producer = KafkaProducer(
        **connection_kwargs(),
        value_serializer=JsonSerializer(),
        key_serializer=KeySerializer(),
    )

    print(f"Subscribed to '{TOPIC_SENSORS}'. Ctrl+C to stop.")

    try:
        for record in consumer:
            data = record.value
            sensor_id = data["sensor_id"]
            temperature = data["temperature"]
            humidity = data["humidity"]
            print(f"Received: {data}")

            if temperature > TEMPERATURE_MAX:
                alert = build_alert(
                    data,
                    f"Temperature {temperature}°C exceeds the threshold of {TEMPERATURE_MAX}°C",
                )
                print(f"  FILTERED (temperature): {alert}")
                producer.send(TOPIC_TEMPERATURE_ALERTS, key=sensor_id, value=alert)
                print(f"  -> sent to '{TOPIC_TEMPERATURE_ALERTS}'")

            if humidity > HUMIDITY_MAX or humidity < HUMIDITY_MIN:
                if humidity > HUMIDITY_MAX:
                    text = (
                        f"Humidity {humidity}% exceeds the threshold of {HUMIDITY_MAX}%"
                    )
                else:
                    text = f"Humidity {humidity}% is below the threshold of {HUMIDITY_MIN}%"
                alert = build_alert(data, text)
                print(f"  FILTERED (humidity): {alert}")
                producer.send(TOPIC_HUMIDITY_ALERTS, key=sensor_id, value=alert)
                print(f"  -> sent to '{TOPIC_HUMIDITY_ALERTS}'")

            producer.flush()
    except KeyboardInterrupt:
        print("\nProcessor stopped.")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
