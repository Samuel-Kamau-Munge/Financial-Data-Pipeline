import sys
import json
import os
import pandas as pd

# Try confluent_kafka first, fall back to kafka-python
try:
    from confluent_kafka import Producer as _ConfluentProducer
    HAS_CONFLUENT = True
except Exception:
    HAS_CONFLUENT = False
    try:
        from kafka import KafkaProducer as _KafkaPythonProducer
        HAS_KAFKA_PYTHON = True
    except Exception:
        HAS_KAFKA_PYTHON = False

if not (HAS_CONFLUENT or HAS_KAFKA_PYTHON):
    raise ImportError("confluent_kafka is not installed and kafka-python is not available; please install one of them.")

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'financial-accounts')
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:9092')

# Create producer instance depending on available library
if HAS_CONFLUENT:
    p = _ConfluentProducer({'bootstrap.servers': BOOTSTRAP_SERVERS})
    _PRODUCER_TYPE = 'confluent'
else:
    # kafka-python expects a list/str bootstrap_servers and value serializer
    p = _KafkaPythonProducer(bootstrap_servers=BOOTSTRAP_SERVERS.split(','), value_serializer=lambda v: v)
    _PRODUCER_TYPE = 'kafka-python'


def delivery_report_confluent(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def send_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Source file not found: {path}")

    df = pd.read_excel(path, engine='openpyxl')

    for _, row in df.iterrows():
        # Normalize payload keys expected by consumer
        payload = {
            'Status': row.get('Status') if 'Status' in row.index else row.get('status'),
            'Amount': row.get('Amount') if 'Amount' in row.index else row.get('amount'),
            'Customer': row.get('Customer') if 'Customer' in row.index else row.get('customer'),
        }

        # Convert pandas/NumPy types to JSON-serializable
        for k, v in payload.items():
            if pd.isna(v):
                payload[k] = None
            else:
                # keep as native python types
                payload[k] = v

        payload_bytes = json.dumps(payload, default=str).encode('utf-8')

        if _PRODUCER_TYPE == 'confluent':
            p.produce(KAFKA_TOPIC, payload_bytes, callback=delivery_report_confluent)
        else:
            # kafka-python: send returns a future
            future = p.send(KAFKA_TOPIC, payload_bytes)

            # add simple callbacks if available
            try:
                future.add_callback(lambda rec: print(f"Message delivered to {rec.topic} [{rec.partition}]"))
                future.add_errback(lambda exc: print(f"Message delivery failed: {exc}"))
            except Exception:
                # some kafka-python versions return different future types; ignore if not supported
                pass

    # flush producers
    try:
        p.flush()
    except Exception:
        try:
            # confluent has flush with timeout
            if _PRODUCER_TYPE == 'confluent':
                p.flush()
        except Exception:
            pass


if __name__ == '__main__':
    source = sys.argv[1] if len(sys.argv) > 1 else r"C:\Finance Data\Accounts-Receivable.xlsx"
    try:
        send_file(source)
    except FileNotFoundError as e:
        print(str(e))
        sys.exit(2)
    except Exception as e:
        print(f"Error sending file: {e}")
        sys.exit(1)
