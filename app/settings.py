BOOTSTRAP_SERVERS = "localhost:9092"

INCOMING_TOPIC = "orders.incoming"
RETRY_TOPIC = "orders.retry"
DEAD_TOPIC = "orders.dead"

PROCESSOR_GROUP = "order-pipeline-processor"
DLQ_VIEWER_GROUP = "order-pipeline-dlq-viewer"

RETRY_HEADER = "x-retry-attempt"

MAX_RETRY_ATTEMPTS = 3

BASE_RETRY_DELAY_SECONDS = 2
