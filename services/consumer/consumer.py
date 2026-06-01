import json
from typing import Optional

# Try to import confluent_kafka first, fall back to kafka-python
try:
    from confluent_kafka import Consumer as _ConfluentConsumer
    HAS_CONFLUENT = True
except Exception:
    HAS_CONFLUENT = False
    try:
        from kafka import KafkaConsumer as _KafkaPythonConsumer
        HAS_KAFKA_PYTHON = True
    except Exception:
        HAS_KAFKA_PYTHON = False


if not (HAS_CONFLUENT or HAS_KAFKA_PYTHON):
    raise ImportError("confluent_kafka is not installed and kafka-python is not available; please install one of them.")


class Consumer:
    """Compatibility wrapper around either confluent_kafka.Consumer or kafka.KafkaConsumer.

    Exposes: subscribe(topics), poll(timeout_seconds) -> message-like object with
    .error() and .value(), commit(), close()
    """

    def __init__(self, config: dict):
        if HAS_CONFLUENT:
            # confluent_kafka expects the config keys as-is
            confluent_conf = {k: v for k, v in config.items()}
            self._type = "confluent"
            self._consumer = _ConfluentConsumer(confluent_conf)
        else:
            brokers = config.get("bootstrap.servers") or config.get("bootstrap_servers") or "localhost:9092"
            group = config.get("group.id") or config.get("group_id")
            auto_offset = config.get("auto.offset.reset", "earliest")
            self._type = "kafka-python"
            self._consumer = _KafkaPythonConsumer(
                bootstrap_servers=brokers.split(","),
                group_id=group,
                auto_offset_reset=auto_offset,
                enable_auto_commit=False,
            )

    def subscribe(self, topics):
        # both clients implement subscribe
        self._consumer.subscribe(topics)

    def poll(self, timeout: float):
        """Return a small compatibility wrapper or None."""
        if self._type == "confluent":
            msg = self._consumer.poll(timeout)
            if msg is None:
                return None

            # confluent message already implements .error() and .value()
            return msg
        else:
            # kafka-python: poll returns dict of partition->list(records)
            records = self._consumer.poll(timeout_ms=int(timeout * 1000))
            for tp, msgs in records.items():
                if msgs:
                    m = msgs[0]

                    class Msg:
                        def __init__(self, value):
                            self._value = value

                        def error(self):
                            return False

                        def value(self):
                            return self._value

                    return Msg(m.value)
            return None

    def commit(self):
        try:
            # both clients expose commit
            self._consumer.commit()
        except Exception:
            # ignore commit errors here (callers may choose to log)
            pass

    def close(self):
        try:
            self._consumer.close()
        except Exception:
            pass


from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "financial-accounts"

c = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'group.id': 'consumer-group-1',
    'auto.offset.reset': 'earliest'
})

c.subscribe([KAFKA_TOPIC])

engine = create_engine('postgresql+psycopg2://postgres:postgres@postgres:5432/financial')
metadata = MetaData()
raw_table = Table('raw_accounts_receivable', metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('Status', String(50)),
                  Column('Amount', String(50)),
                  Column('Customer', String(255))
                  )
metadata.create_all(engine)

try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        # confluent messages expose .error(); kafka-python wrapped Msg returns False
        try:
            err = msg.error()
        except Exception:
            err = False
        if err:
            print(f"Consumer error: {err}")
            continue

        raw = msg.value()
        if raw is None:
            continue
        if isinstance(raw, bytes):
            payload_text = raw.decode('utf-8')
        else:
            payload_text = str(raw)

        data = json.loads(payload_text)
        ins = raw_table.insert().values(Status=data.get('Status'), Amount=data.get('Amount'), Customer=data.get('Customer'))
        with engine.connect() as conn:
            conn.execute(ins)
            conn.commit()

        # commit offset after successfully writing to DB
        try:
            c.commit()
        except Exception:
            pass
except KeyboardInterrupt:
    pass
finally:
    c.close()
