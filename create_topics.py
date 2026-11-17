from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import (
    TOPIC_HUMIDITY_ALERTS,
    TOPIC_NAME_SPACE,
    TOPIC_SENSORS,
    TOPIC_TEMPERATURE_ALERTS,
    connection_kwargs,
)

NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1
TOPICS = (TOPIC_SENSORS, TOPIC_TEMPERATURE_ALERTS, TOPIC_HUMIDITY_ALERTS)


def main() -> None:
    admin_client = KafkaAdminClient(**connection_kwargs())
    # admin_client.delete_topics(admin_client.list_topics())

    for name in TOPICS:
        topic = NewTopic(
            name=name,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
        )
        try:
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{name}' created successfully.")
        except TopicAlreadyExistsError:
            print(f"Topic '{name}' already exists.")

    print(f"\nTopics containing '{TOPIC_NAME_SPACE}':")
    [print(topic) for topic in admin_client.list_topics() if TOPIC_NAME_SPACE in topic]

    admin_client.close()


if __name__ == "__main__":
    main()
