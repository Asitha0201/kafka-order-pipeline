cat > README.md <<'EOF'
# Kafka Order Processing Pipeline

A real-time order-processing pipeline built using **Apache Kafka, Python, and Apache Avro**.

The system publishes purchase orders to Kafka, processes them in real time, calculates a running average of successfully processed order prices, retries temporary failures, and routes permanently failed messages to a Dead Letter Queue.

---

## Features

- Apache Kafka producer and consumer
- Avro binary serialization and deserialization
- Real-time running average of processed order prices
- Temporary failure detection
- Bounded retry handling
- Retry backoff
- Retry count stored in Kafka message headers
- Dead Letter Queue for permanent failures
- Avro-serialized DLQ records
- Docker-based Kafka deployment
- Incremental Git development
- Repeatable 20-order live demonstration

---

# 1. System Architecture

```text
                         +----------------------+
                         |    Order Producer    |
                         +----------+-----------+
                                    |
                                    | Avro order
                                    v
                         +----------------------+
                         |   orders.incoming    |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         |   Order Processor    |
                         +----------+-----------+
                                    |
              +---------------------+----------------------+
              |                     |                      |
              v                     v                      v
           SUCCESS           TEMPORARY FAILURE      PERMANENT FAILURE
              |                     |                      |
              v                     v                      v
      Running Average         orders.retry            orders.dead
                                    |                      |
                                    |                      v
                                    |                DLQ Viewer
                                    |
                                    +-----------> Order Processor
```

---

# 2. Order Message Format

Every order follows the Avro schema stored in:

```text
schemas/order.avsc
```

Each order contains three fields:

| Field | Type | Description |
|---|---|---|
| `orderId` | string | Unique identifier for the order |
| `product` | string | Name of the purchased product |
| `price` | float | Price of the order |

Example logical order:

```json
{
  "orderId": "2001",
  "product": "Keyboard",
  "price": 149.95
}
```

The Python dictionary is converted into binary Avro data before being sent through Kafka.

---

# 3. Kafka Topics

The system uses three Kafka topics.

## `orders.incoming`

Contains newly generated order messages.

```text
Producer
   |
   v
orders.incoming
```

---

## `orders.retry`

Contains messages that experienced a temporary processing failure.

```text
Processor
   |
   X Temporary Failure
   |
   v
orders.retry
   |
   v
Processor
```

---

## `orders.dead`

Contains orders that cannot be successfully processed.

```text
Processor
   |
   X Permanent Failure
   |
   v
orders.dead
   |
   v
DLQ Viewer
```

---

# 4. Project Structure

```text
kafka-order-pipeline/
│
├── .gitignore
├── compose.yaml
├── requirements.txt
├── README.md
│
├── schemas/
│   ├── order.avsc
│   └── failed_order.avsc
│
└── app/
    ├── settings.py
    ├── avro_codec.py
    ├── topic_admin.py
    ├── order_producer.py
    ├── order_processor.py
    └── dlq_viewer.py
```

---

# 5. Technologies

The project uses:

- Apache Kafka
- Kafka KRaft mode
- Docker
- Docker Compose
- Python
- `confluent-kafka`
- Apache Avro
- `fastavro`
- Git
- GitHub
- GitHub Codespaces

---

# 6. Avro Serialization

The project uses Avro binary serialization without requiring a separate Schema Registry service.

The order schema is stored in:

```text
schemas/order.avsc
```

The failed-order schema is stored in:

```text
schemas/failed_order.avsc
```

## Producer serialization flow

```text
Python Dictionary
       |
       v
Avro Schema
       |
       v
fastavro
       |
       v
Avro Binary Data
       |
       v
Kafka
```

## Consumer deserialization flow

```text
Kafka
  |
  v
Avro Binary Data
  |
  v
fastavro
  |
  v
Python Dictionary
```

The common Avro encode/decode functionality is implemented in:

```text
app/avro_codec.py
```

---

# 7. Real-Time Running Average

The order processor maintains a running average of all successfully processed order prices.

The calculation is:

```text
                  Total Price of Successful Orders
Running Average = --------------------------------
                   Number of Successful Orders
```

Example:

```text
Order 1 price = 100
Order 2 price = 200
Order 3 price = 300

Total = 600

Running Average = 600 / 3
                = 200
```

Only successfully processed orders contribute to the running average.

Temporary failures contribute only after they eventually succeed.

Permanent failures sent to the Dead Letter Queue are not included.

---

# 8. Failure Simulation Strategy

The project uses deterministic order IDs to demonstrate temporary and permanent failures.

This makes the live demonstration predictable and repeatable.

## Temporary failure rule

An order ID ending in:

```text
5
```

is treated as a temporary processing failure.

Examples:

```text
2005
2015
```

These orders fail initially but eventually succeed after retrying.

---

## Permanent failure rule

An order ID ending in:

```text
0
```

is treated as a permanent processing failure.

Examples:

```text
2010
2020
```

These orders are sent directly to the Dead Letter Queue.

---

# 9. Retry Mechanism

Temporary failures are represented by:

```text
TemporaryOrderFailure
```

The retry count is stored in the Kafka message header:

```text
x-retry-attempt
```

The processor reads this header whenever a retry message is consumed.

The maximum configured number of retries is:

```text
MAX_RETRY_ATTEMPTS = 3
```

The current demonstration is designed so that orders ending in `5` succeed on retry attempt `2`.

---

# 10. Retry Backoff

Retry processing uses increasing delays.

The delay is calculated using:

```text
delay = BASE_RETRY_DELAY_SECONDS ^ retry_attempt
```

With:

```text
BASE_RETRY_DELAY_SECONDS = 2
```

the first two retry delays are:

```text
Retry 1 -> 2 seconds
Retry 2 -> 4 seconds
```

The retry flow is:

```text
Order
  |
  v
Processor
  |
  X Temporary Failure
  |
  v
attempt = 1
  |
  v
Wait 2 seconds
  |
  v
orders.retry
  |
  v
Processor
  |
  X Temporary Failure
  |
  v
attempt = 2
  |
  v
Wait 4 seconds
  |
  v
orders.retry
  |
  v
Processor
  |
  v
SUCCESS
```

---

# 11. Dead Letter Queue

Permanent failures are represented by:

```text
PermanentOrderFailure
```

These records are routed to:

```text
orders.dead
```

The DLQ message is also serialized using Avro.

A failed-order record contains:

| Field | Description |
|---|---|
| `orderId` | Original order ID |
| `product` | Original product |
| `price` | Original price |
| `reason` | Failure reason |
| `errorClass` | Exception type |
| `attempt` | Retry attempt count |
| `failedAt` | UTC failure timestamp |

Example:

```text
DEAD LETTER
Order ID: 2010
Product: Webcam
Price: 275.40
Error: PermanentOrderFailure
Reason: Order failed permanent business validation.
Attempt: 0
Failed At: 2026-09-18T10:15:20.123456+00:00
```

The DLQ can be monitored using:

```text
app/dlq_viewer.py
```

---

# 12. Prerequisites

The project is designed to run inside a GitHub Codespace.

Required software:

```text
Docker
Docker Compose
Python 3
Git
```

GitHub Codespaces already provides the Docker environment required by this project.

---

# 13. Start the Project

Open the repository in GitHub Codespaces.

Confirm the repository location:

```bash
pwd
```

Expected:

```text
/workspaces/kafka-order-pipeline
```

---

# 14. Create the Python Environment

Create a virtual environment:

```bash
python3 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install the dependencies:

```bash
pip install -r requirements.txt
```

The main Python dependencies are:

```text
confluent-kafka
fastavro
```

---

# 15. Start Kafka

Run:

```bash
docker compose up -d
```

Check the broker:

```bash
docker compose ps
```

Expected state:

```text
order-kafka    Up ... (healthy)
```

If Kafka still shows:

```text
health: starting
```

wait a few seconds and run:

```bash
docker compose ps
```

again.

---

# 16. Create Kafka Topics

Run:

```bash
python app/topic_admin.py
```

Expected output:

```text
Created topic: orders.incoming
Created topic: orders.retry
Created topic: orders.dead
```

If the topics already exist, the application reports that instead of failing.

Verify the topics:

```bash
docker exec order-kafka kafka-topics \
  --bootstrap-server broker:29092 \
  --list
```

Expected:

```text
orders.dead
orders.incoming
orders.retry
```

---

# 17. Live Demonstration

The final demonstration uses:

```text
20 original orders
```

with order IDs:

```text
2001 - 2020
```

The workload intentionally produces:

```text
16 normal orders
2 temporary failures
2 permanent failures
```

The temporary failures eventually succeed.

Therefore:

```text
Original orders          = 20
Successfully processed   = 18
Dead-lettered orders     = 2
Retry-topic messages     = 4
```

---

# 18. Demonstration Order Sequence

The producer generates:

```text
2001
2002
2003
2004
2005  <- Temporary failure

2006
2007
2008
2009
2010  <- Permanent failure

2011
2012
2013
2014
2015  <- Temporary failure

2016
2017
2018
2019
2020  <- Permanent failure
```

Therefore:

```text
2005 -> retry -> retry -> success

2010 -> Dead Letter Queue

2015 -> retry -> retry -> success

2020 -> Dead Letter Queue
```

---

# 19. Terminal 1 - Order Processor

Open the first Codespaces terminal.

Run:

```bash
cd /workspaces/kafka-order-pipeline
```

Activate the virtual environment:

```bash
source .venv/bin/activate
```

Start the processor:

```bash
python app/order_processor.py
```

The processor listens to:

```text
orders.incoming
orders.retry
```

Normal processing should produce output similar to:

```text
SUCCESS | id=2001 | product=Keyboard | price=392.67 | attempt=0
RUNNING AVERAGE = 392.67
SUCCESSFUL ORDERS = 1
------------------------------------------------------------
```

Leave this terminal running.

---

# 20. Terminal 2 - Dead Letter Queue Viewer

Open a second Codespaces terminal.

Run:

```bash
cd /workspaces/kafka-order-pipeline
```

Activate:

```bash
source .venv/bin/activate
```

Start:

```bash
python app/dlq_viewer.py
```

Expected initial output:

```text
Monitoring orders.dead
```

Leave this terminal running.

---

# 21. Terminal 3 - Order Producer

Open a third Codespaces terminal.

Run:

```bash
cd /workspaces/kafka-order-pipeline
```

Activate:

```bash
source .venv/bin/activate
```

Start the 20-order demonstration:

```bash
python app/order_producer.py \
  --count 20 \
  --interval 0.5 \
  --start-id 2001
```

The producer uses a fixed random seed by default so the generated prices are reproducible between demonstrations.

---

# 22. Expected Temporary Failure Output

For order:

```text
2005
```

Terminal 1 should show something similar to:

```text
TEMPORARY FAILURE | id=2005 | attempt=1/3
Waiting 2 seconds...
RETRY PUBLISHED -> orders.retry
```

Later:

```text
TEMPORARY FAILURE | id=2005 | attempt=2/3
Waiting 4 seconds...
RETRY PUBLISHED -> orders.retry
```

Finally:

```text
SUCCESS | id=2005 | ... | attempt=2
```

The same process occurs for:

```text
2015
```

---

# 23. Expected Permanent Failure Output

For:

```text
2010
```

Terminal 1 should show:

```text
PERMANENT FAILURE | id=2010
Reason: Order failed permanent business validation.
DEAD LETTER PUBLISHED -> orders.dead | id=2010
```

The same happens for:

```text
2020
```

---

# 24. Expected DLQ Viewer Output

Terminal 2 should display:

```text
DEAD LETTER
Order ID: 2010
Product: ...
Price: ...
Error: PermanentOrderFailure
Reason: Order failed permanent business validation.
Attempt: 0
Failed At: ...
------------------------------------------------------------
```

A second DLQ message should later appear for:

```text
2020
```

---

# 25. Verify Kafka Message Counts

For a completely fresh Kafka environment, one full demonstration should produce the following topic offsets.

## Incoming orders

```bash
docker exec order-kafka kafka-get-offsets \
  --bootstrap-server broker:29092 \
  --topic orders.incoming
```

Expected:

```text
orders.incoming:0:20
```

---

## Retry messages

```bash
docker exec order-kafka kafka-get-offsets \
  --bootstrap-server broker:29092 \
  --topic orders.retry
```

Expected:

```text
orders.retry:0:4
```

Why?

```text
2 temporarily failing orders
×
2 retries each
=
4 retry messages
```

---

## Dead-letter messages

```bash
docker exec order-kafka kafka-get-offsets \
  --bootstrap-server broker:29092 \
  --topic orders.dead
```

Expected:

```text
orders.dead:0:2
```

If earlier tests were already performed, the offsets will be larger because Kafka offsets are cumulative.

---

# 26. Demonstration Summary

For one fresh 20-order run:

```text
Original orders         = 20

Normal orders           = 16

Temporary failures      = 2
Temporary orders succeed after retry

Permanent failures      = 2

Successful processing   = 18

Retry messages          = 4

Dead Letter messages    = 2
```

---

# 27. Check Git History

The project was developed incrementally.

Run:

```bash
git log --oneline --graph --decorate
```

The repository history should show separate development stages such as:

```text
docs: document architecture and demonstration procedure

feat: route permanent failures to dead letter topic

feat: implement bounded retry processing

feat: calculate streaming order price average

feat: implement Avro order publisher

feat: define Avro orders and Kafka topics

infra: configure single-node Kafka broker

chore: create order streaming project
```

This keeps infrastructure, producer, aggregation, retry handling, DLQ handling, and documentation as separate development stages.

---

# 28. Stop the Demonstration

Stop the processor with:

```text
Ctrl + C
```

Stop the DLQ viewer with:

```text
Ctrl + C
```

Then stop Kafka:

```bash
docker compose down
```

---

# 29. Clean Demo Restart

To perform a completely fresh demonstration:

```bash
docker compose down --remove-orphans
```

Then:

```bash
docker compose up -d
```

Wait until Kafka is healthy:

```bash
docker compose ps
```

Create the topics again:

```bash
python app/topic_admin.py
```

Then start:

```text
Terminal 1 -> order_processor.py
Terminal 2 -> dlq_viewer.py
Terminal 3 -> order_producer.py
```

This produces a clean demonstration with:

```text
orders.incoming:0:20
orders.retry:0:4
orders.dead:0:2
```

---

# 30. Important Implementation Note

Kafka is responsible for:

- storing messages
- transporting messages
- maintaining topics
- managing offsets
- enabling producer/consumer communication

The Python application is responsible for:

- Avro encoding and decoding
- determining processing success or failure
- retry policy
- retry backoff
- retry attempt tracking
- running average calculation
- detecting permanent failures
- routing failed records to the Dead Letter Queue

The temporary and permanent failure conditions are intentionally deterministic so the behavior can be demonstrated reliably during evaluation.