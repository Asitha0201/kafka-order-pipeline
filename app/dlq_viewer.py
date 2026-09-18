from confluent_kafka import Consumer

from avro_codec import (
    decode_record,
    load_avro_schema,
)

from settings import (
    BOOTSTRAP_SERVERS,
    DEAD_TOPIC,
    DLQ_VIEWER_GROUP,
)


def main():

    schema = load_avro_schema(
        "failed_order.avsc"
    )

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                DLQ_VIEWER_GROUP,

            "auto.offset.reset":
                "earliest",
        }
    )

    consumer.subscribe(
        [DEAD_TOPIC]
    )

    print(
        f"Monitoring {DEAD_TOPIC}"
    )

    print()

    try:

        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():

                print(
                    f"Kafka error: "
                    f"{message.error()}"
                )

                continue

            failed = decode_record(
                message.value(),
                schema,
            )

            print("DEAD LETTER")

            print(
                f"Order ID: "
                f"{failed['orderId']}"
            )

            print(
                f"Product: "
                f"{failed['product']}"
            )

            print(
                f"Price: "
                f"{failed['price']:.2f}"
            )

            print(
                f"Error: "
                f"{failed['errorClass']}"
            )

            print(
                f"Reason: "
                f"{failed['reason']}"
            )

            print(
                f"Attempt: "
                f"{failed['attempt']}"
            )

            print(
                f"Failed At: "
                f"{failed['failedAt']}"
            )

            print("-" * 60)

    except KeyboardInterrupt:

        print(
            "\nDLQ viewer stopped."
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()
