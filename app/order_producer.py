import argparse
import random
import time

from confluent_kafka import Producer

from avro_codec import (
    encode_record,
    load_avro_schema,
)

from settings import (
    BOOTSTRAP_SERVERS,
    INCOMING_TOPIC,
)


PRODUCTS = [
    "Keyboard",
    "Mouse",
    "Monitor",
    "Headset",
    "Webcam",
]


def delivery_callback(error, message):

    if error:

        print(
            f"Delivery failed: {error}"
        )

        return

    print(
        f"Delivered -> "
        f"{message.topic()} "
        f"[partition={message.partition()}, "
        f"offset={message.offset()}]"
    )


def main():

    parser = argparse.ArgumentParser(
        description="Generate Avro order events."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=20,
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
    )

    parser.add_argument(
        "--start-id",
        type=int,
        default=2001,
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
    )

    args = parser.parse_args()

    random_source = random.Random(
        args.seed
    )

    schema = load_avro_schema(
        "order.avsc"
    )

    producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )

    print()
    print(
        f"Generating {args.count} orders..."
    )
    print()

    for index in range(args.count):

        order_id = (
            args.start_id + index
        )

        order = {
            "orderId":
                str(order_id),

            "product":
                PRODUCTS[
                    index % len(PRODUCTS)
                ],

            "price":
                round(
                    random_source.uniform(
                        25.0,
                        600.0,
                    ),
                    2,
                ),
        }

        encoded_order = encode_record(
            order,
            schema,
        )

        print(
            f"ORDER -> "
            f"id={order['orderId']} | "
            f"product={order['product']} | "
            f"price={order['price']:.2f}"
        )

        producer.produce(
            topic=INCOMING_TOPIC,
            key=order["orderId"],
            value=encoded_order,
            callback=delivery_callback,
        )

        producer.poll(0)

        time.sleep(
            args.interval
        )

    producer.flush()

    print()
    print("Producer finished.")


if __name__ == "__main__":
    main()
