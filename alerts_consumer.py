from kafka import KafkaConsumer

from configs import (
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_NAME_SPACE,
    TOPIC_TEMPERATURE_ALERTS,
    JsonDeserializer,
    connection_kwargs,
)


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC_TEMPERATURE_ALERTS,
        TOPIC_HUMIDITY_ALERTS,
        **connection_kwargs(),
        value_deserializer=JsonDeserializer(),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=f"{TOPIC_NAME_SPACE}_alerts_reader",
    )

    print(
        f"Subscribed to '{TOPIC_TEMPERATURE_ALERTS}' and '{TOPIC_HUMIDITY_ALERTS}'. "
        "Ctrl+C to stop."
    )

    try:
        for record in consumer:
            alert = record.value
            print(
                f"[{record.topic}] sensor={alert['sensor_id']} "
                f"t={alert['temperature']}°C h={alert['humidity']}% "
                f"time={alert['timestamp']} | {alert['message']}"
            )
    except KeyboardInterrupt:
        print("\nAlerts reader stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
