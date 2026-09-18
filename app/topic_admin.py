from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

from settings import (
    BOOTSTRAP_SERVERS,
    INCOMING_TOPIC,
    RETRY_TOPIC,
    DEAD_TOPIC,
)


def main():

    admin = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS
        }
    )

    topics = [
        NewTopic(
            INCOMING_TOPIC,
            num_partitions=1,
            replication_factor=1,
        ),

        NewTopic(
            RETRY_TOPIC,
            num_partitions=1,
            replication_factor=1,
        ),

        NewTopic(
            DEAD_TOPIC,
            num_partitions=1,
            replication_factor=1,
        ),
    ]

    results = admin.create_topics(topics)

    for topic_name, future in results.items():

        try:

            future.result()

            print(
                f"Created topic: {topic_name}"
            )

        except KafkaException as exc:

            error = exc.args[0]

            if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:

                print(
                    f"Topic already exists: {topic_name}"
                )

            else:

                raise


if __name__ == "__main__":
    main()
