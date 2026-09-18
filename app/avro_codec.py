import io
import json
from pathlib import Path

from fastavro import parse_schema
from fastavro import schemaless_reader
from fastavro import schemaless_writer


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIRECTORY = PROJECT_ROOT / "schemas"


def load_avro_schema(filename):
    path = SCHEMA_DIRECTORY / filename

    with open(path, "r", encoding="utf-8") as file:
        schema = json.load(file)

    return parse_schema(schema)


def encode_record(record, schema):
    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        record,
    )

    return buffer.getvalue()


def decode_record(data, schema):
    buffer = io.BytesIO(data)

    return schemaless_reader(
        buffer,
        schema,
    )
