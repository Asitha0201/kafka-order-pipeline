from dataclasses import dataclass

from confluent_kafka import Consumer

from avro_codec import (
    decode_record,
    load_avro_schema,
)

from settings import (
    BOOTSTRAP_SERVERS,
    INCOMING_TOPIC,
    PROCESSOR_GROUP,
)


@dataclass
class RunningAverage:

    count: int = 0
    total: float = 0.0

    def add(self, value):

        self.count += 1

        self.total += float(value)

        return self.total / self.count


def main():

    schema = load_avro_schema(
        "order.avsc"
    )

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                PROCESSOR_GROUP,

            "auto.offset.reset":
                "earliest",

            "enable.auto.commit":
                False,
        }
    )

    consumer.subscribe(
        [INCOMING_TOPIC]
    )

    statistics = RunningAverage()

    print(
        f"Listening on {INCOMING_TOPIC}"
    )

    print(
        "Press Ctrl+C to stop."
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

            order = decode_record(
                message.value(),
                schema,
            )

            average = statistics.add(
                order["price"]
            )

            print(
                f"PROCESSED | "
                f"id={order['orderId']} | "
                f"product={order['product']} | "
                f"price={order['price']:.2f}"
            )

            print(
                f"RUNNING AVERAGE = "
                f"{average:.2f}"
            )

            print(
                f"PROCESSED COUNT = "
                f"{statistics.count}"
            )

            print("-" * 60)

            consumer.commit(
                message=message,
                asynchronous=False,
            )

    except KeyboardInterrupt:

        print(
            "\nProcessor stopped."
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()
