"""Спільні налаштування для всіх скриптів ДЗ."""

import json

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic, headers, data):
        return json.dumps(data).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic, headers, data):
        return json.loads(data.decode("utf-8"))


class KeySerializer(Serializer):
    def serialize(self, topic, headers, data):
        return str(data).encode("utf-8")


TOPIC_NAME_SPACE = "goit-de-hw-05"

TOPIC_SENSORS = f"{TOPIC_NAME_SPACE}_building_sensors"
TOPIC_TEMPERATURE_ALERTS = f"{TOPIC_NAME_SPACE}_temperature_alerts"
TOPIC_HUMIDITY_ALERTS = f"{TOPIC_NAME_SPACE}_humidity_alerts"

TEMPERATURE_MAX = 40
HUMIDITY_MAX = 80
HUMIDITY_MIN = 20

kafka_config = {
    "bootstrap_servers": ["localhost:9092"],
    "security_protocol": "PLAINTEXT",
}


def connection_kwargs() -> dict:
    """Параметри підключення, спільні для admin / producer / consumer."""
    kwargs = {
        "bootstrap_servers": kafka_config["bootstrap_servers"],
        "security_protocol": kafka_config["security_protocol"],
    }

    return kwargs
