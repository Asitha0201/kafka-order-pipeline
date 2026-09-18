import time
from dataclasses import dataclass
from datetime import (
    datetime,
    timezone,
)

from confluent_kafka import (
    Consumer,
    Producer,
)

from avro_codec import (
    decode_record,
    encode_record,
    load_avro_schema,
)

from settings import (
    BASE_RETRY_DELAY_SECONDS,
    BOOTSTRAP_SERVERS,
    DEAD_TOPIC,
    INCOMING_TOPIC,
    MAX_RETRY_ATTEMPTS,
    PROCESSOR_GROUP,
    RETRY_HEADER,
    RETRY_TOPIC,
)


class TemporaryOrderFailure(Exception):
    pass


class PermanentOrderFailure(Exception):
    pass


@dataclass
class RunningAverage:

    count: int = 0
    total: float = 0.0

    def add(self, value):

        self.count += 1

        self.total += float(value)

        return self.total / self.count


def read_retry_attempt(headers):

    if not headers:
        return 0

    for key, value in headers:

        if key == RETRY_HEADER:

            if isinstance(value, bytes):
                value = value.decode(
                    "utf-8"
                )

            return int(value)

    return 0


def validate_order(order, attempt):

    order_number = int(
        order["orderId"]
    )

    # IDs ending in 0 represent
    # permanently invalid orders.

    if order_number % 10 == 0:

        raise PermanentOrderFailure(
            "Order failed permanent business validation."
        )

    # IDs ending in 5 simulate
    # temporary downstream failures.

    if (
        order_number % 10 == 5
        and attempt < 2
    ):

        raise TemporaryOrderFailure(
            "Temporary downstream service unavailable."
        )


def send_to_dead_letter(
    producer,
    order,
    error,
    attempt,
    failed_schema,
):

    record = {

        "orderId":
            order["orderId"],

        "product":
            order["product"],

        "price":
            float(
                order["price"]
            ),

        "reason":
            str(error),

        "errorClass":
            type(error).__name__,

        "attempt":
            attempt,

        "failedAt":
            datetime.now(
                timezone.utc
            ).isoformat(),
    }

    producer.produce(
        topic=DEAD_TOPIC,
        key=order["orderId"],
        value=encode_record(
            record,
            failed_schema,
        ),
    )

    producer.flush()

    print(
        f"DEAD LETTER PUBLISHED -> "
        f"{DEAD_TOPIC} | "
        f"id={order['orderId']}"
    )


def main():

    order_schema = load_avro_schema(
        "order.avsc"
    )

    failed_schema = load_avro_schema(
        "failed_order.avsc"
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

    retry_producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )

    dead_letter_producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )

    consumer.subscribe(
        [
            INCOMING_TOPIC,
            RETRY_TOPIC,
        ]
    )

    statistics = RunningAverage()

    print(
        f"Listening on "
        f"{INCOMING_TOPIC} and "
        f"{RETRY_TOPIC}"
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
                order_schema,
            )

            attempt = read_retry_attempt(
                message.headers()
            )

            try:

                validate_order(
                    order,
                    attempt,
                )

                average = statistics.add(
                    order["price"]
                )

                print(
                    f"SUCCESS | "
                    f"id={order['orderId']} | "
                    f"product={order['product']} | "
                    f"price={order['price']:.2f} | "
                    f"attempt={attempt}"
                )

                print(
                    f"RUNNING AVERAGE = "
                    f"{average:.2f}"
                )

                print(
                    f"SUCCESSFUL ORDERS = "
                    f"{statistics.count}"
                )

                print("-" * 60)

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except TemporaryOrderFailure as error:

                next_attempt = (
                    attempt + 1
                )

                print(
                    f"TEMPORARY FAILURE | "
                    f"id={order['orderId']} | "
                    f"attempt="
                    f"{next_attempt}/"
                    f"{MAX_RETRY_ATTEMPTS}"
                )

                if (
                    next_attempt
                    <= MAX_RETRY_ATTEMPTS
                ):

                    delay = (
                        BASE_RETRY_DELAY_SECONDS
                        ** next_attempt
                    )

                    print(
                        f"Waiting {delay} seconds..."
                    )

                    time.sleep(delay)

                    retry_producer.produce(
                        topic=RETRY_TOPIC,
                        key=order["orderId"],
                        value=encode_record(
                            order,
                            order_schema,
                        ),
                        headers=[
                            (
                                RETRY_HEADER,
                                str(
                                    next_attempt
                                ).encode(
                                    "utf-8"
                                ),
                            )
                        ],
                    )

                    retry_producer.flush()

                    print(
                        f"RETRY PUBLISHED -> "
                        f"{RETRY_TOPIC}"
                    )

                else:

                    send_to_dead_letter(
                        dead_letter_producer,
                        order,
                        error,
                        attempt,
                        failed_schema,
                    )

                print("-" * 60)

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

            except PermanentOrderFailure as error:

                print(
                    f"PERMANENT FAILURE | "
                    f"id={order['orderId']}"
                )

                print(
                    f"Reason: {error}"
                )

                send_to_dead_letter(
                    dead_letter_producer,
                    order,
                    error,
                    attempt,
                    failed_schema,
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

        retry_producer.flush()

        dead_letter_producer.flush()


if __name__ == "__main__":
    main()
