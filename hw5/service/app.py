import io
import json
import logging
import os
import random
import struct
import threading
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import boto3
import clickhouse_connect
import psycopg
import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI, HTTPException
from fastavro import parse_schema, schemaless_writer, validation
from pydantic import BaseModel, ConfigDict, Field, field_validator


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("movie-analytics")

BASE_DIR = Path(__file__).resolve().parent
RAW_SCHEMA = json.loads((BASE_DIR / "schema.avsc").read_text())
SCHEMA = parse_schema(RAW_SCHEMA)
EVENT_TYPES = {
    "VIEW_STARTED",
    "VIEW_FINISHED",
    "VIEW_PAUSED",
    "VIEW_RESUMED",
    "LIKED",
    "SEARCHED",
}
DEVICE_TYPES = {"MOBILE", "DESKTOP", "TV", "TABLET"}


class Settings:
    app_role = os.getenv("APP_ROLE", "producer")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    kafka_topic = os.getenv("KAFKA_TOPIC", "movie-events")
    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    clickhouse_database = os.getenv("CLICKHOUSE_DATABASE", "analytics")
    clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
    clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
    postgres_dsn = os.getenv(
        "POSTGRES_DSN",
        "postgresql://app:app@postgres:5432/analytics",
    )
    s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_access_key = os.getenv("S3_ACCESS_KEY", "minio")
    s3_secret_key = os.getenv("S3_SECRET_KEY", "minio123")
    s3_bucket = os.getenv("S3_BUCKET", "movie-analytics")
    aggregation_interval_seconds = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300"))
    export_format = os.getenv("EXPORT_FORMAT", "json").lower()
    scheduler_target_lag_days = int(os.getenv("SCHEDULER_TARGET_LAG_DAYS", "1"))


settings = Settings()
app = FastAPI(title="movie-analytics")
BOOTSTRAP_LOCK = threading.Lock()
BOOTSTRAPPED = False
SCHEDULER_STARTED = False
SCHEMA_ID: int | None = None


class EventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str | None = None
    user_id: str
    movie_id: str
    event_type: str
    timestamp: datetime | None = None
    device_type: str
    session_id: str | None = None
    progress_seconds: int = Field(ge=0, default=0)

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        value = value.upper()
        if value not in EVENT_TYPES:
            raise ValueError(f"unsupported event_type: {value}")
        return value

    @field_validator("device_type")
    @classmethod
    def validate_device_type(cls, value: str) -> str:
        value = value.upper()
        if value not in DEVICE_TYPES:
            raise ValueError(f"unsupported device_type: {value}")
        return value


class GenerateIn(BaseModel):
    users: int = Field(default=5, ge=1, le=50)
    sessions_per_user: int = Field(default=2, ge=1, le=10)


class DayIn(BaseModel):
    day: date | None = None


def utcnow() -> datetime:
    return datetime.now(UTC)


def to_record(event: EventIn) -> dict[str, Any]:
    ts = event.timestamp or utcnow()
    ts = ts.astimezone(UTC)
    record = {
        "event_id": event.event_id or str(uuid.uuid4()),
        "user_id": event.user_id,
        "movie_id": event.movie_id,
        "event_type": event.event_type,
        "timestamp": int(ts.timestamp() * 1000),
        "device_type": event.device_type,
        "session_id": event.session_id or str(uuid.uuid4()),
        "progress_seconds": event.progress_seconds,
    }
    if not validation.validate(record, SCHEMA):
        raise HTTPException(status_code=400, detail="event does not match schema")
    return record


def wait_for_http(url: str, timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            response = requests.get(url, timeout=3)
            if response.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"timeout waiting for {url}")


def wait_for_clickhouse(timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = get_clickhouse_client()
            client.command("SELECT 1")
            client.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("timeout waiting for ClickHouse")


def wait_for_postgres(timeout: int = 90) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("timeout waiting for PostgreSQL")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def get_schema_id() -> int:
    global SCHEMA_ID
    if SCHEMA_ID is not None:
        return SCHEMA_ID
    subject = f"{settings.kafka_topic}-value"
    response = requests.post(
        f"{settings.schema_registry_url}/subjects/{subject}/versions",
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        data=json.dumps({"schema": json.dumps(RAW_SCHEMA), "schemaType": "AVRO"}),
        timeout=10,
    )
    response.raise_for_status()
    schema_id = int(response.json()["id"])
    latest = requests.get(
        f"{settings.schema_registry_url}/subjects/{subject}/versions/latest",
        timeout=10,
    )
    latest.raise_for_status()
    payload = latest.json()
    log.info(
        "schema registered subject=%s version=%s id=%s",
        subject,
        payload.get("version"),
        payload.get("id"),
    )
    SCHEMA_ID = schema_id
    return SCHEMA_ID


def encode_confluent_avro(record: dict[str, Any]) -> bytes:
    schema_id = get_schema_id()
    buffer = io.BytesIO()
    buffer.write(b"\x00")
    buffer.write(struct.pack(">I", schema_id))
    schemaless_writer(buffer, SCHEMA, record)
    return buffer.getvalue()


def create_topic() -> None:
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    future = admin.create_topics(
        [
            NewTopic(
                settings.kafka_topic,
                num_partitions=3,
                replication_factor=2,
                config={"min.insync.replicas": "1"},
            )
        ]
    )
    try:
        future[settings.kafka_topic].result()
        log.info("kafka topic created topic=%s", settings.kafka_topic)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            log.info("kafka topic already exists topic=%s", settings.kafka_topic)
        else:
            raise


def get_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "snappy",
        }
    )


def publish_record(record: dict[str, Any]) -> dict[str, Any]:
    payload = encode_confluent_avro(record)
    key = record["user_id"].encode()
    last_error = None
    for attempt in range(5):
        try:
            producer = get_producer()
            delivery = {}

            def callback(err, msg) -> None:
                delivery["err"] = err
                delivery["msg"] = msg

            producer.produce(settings.kafka_topic, key=key, value=payload, on_delivery=callback)
            producer.flush(15)
            if delivery.get("err") is not None:
                raise RuntimeError(str(delivery["err"]))
            msg = delivery["msg"]
            log.info(
                "event published event_id=%s event_type=%s ts=%s partition=%s offset=%s",
                record["event_id"],
                record["event_type"],
                record["timestamp"],
                msg.partition(),
                msg.offset(),
            )
            return {
                "event_id": record["event_id"],
                "partition": msg.partition(),
                "offset": msg.offset(),
            }
        except Exception as exc:
            last_error = exc
            sleep_for = 2**attempt
            log.warning("publish retry=%s error=%s", attempt + 1, exc)
            time.sleep(sleep_for)
    raise RuntimeError(f"failed to publish after retries: {last_error}")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


def ensure_bucket() -> None:
    client = get_s3_client()
    buckets = {item["Name"] for item in client.list_buckets().get("Buckets", [])}
    if settings.s3_bucket not in buckets:
        client.create_bucket(Bucket=settings.s3_bucket)
        log.info("created bucket=%s", settings.s3_bucket)


def bootstrap_once() -> None:
    global BOOTSTRAPPED
    if BOOTSTRAPPED:
        return
    with BOOTSTRAP_LOCK:
        if BOOTSTRAPPED:
            return
        wait_for_http(f"{settings.schema_registry_url}/subjects")
        wait_for_clickhouse()
        wait_for_postgres()
        create_topic()
        get_schema_id()
        ensure_bucket()
        BOOTSTRAPPED = True


def build_demo_sequence(user_id: str, movie_id: str, device_type: str, session_id: str, start_at: datetime) -> list[dict[str, Any]]:
    ts = start_at.astimezone(UTC)
    steps = [
        ("VIEW_STARTED", 0, 0),
        ("VIEW_PAUSED", 120, 120),
        ("VIEW_RESUMED", 135, 120),
        ("VIEW_FINISHED", 360, 360),
        ("LIKED", 365, 360),
    ]
    result = []
    for event_type, seconds_from_start, progress in steps:
        result.append(
            to_record(
                EventIn(
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=event_type,
                    timestamp=ts + timedelta(seconds=seconds_from_start),
                    device_type=device_type,
                    session_id=session_id,
                    progress_seconds=progress,
                )
            )
        )
    result.append(
        to_record(
            EventIn(
                user_id=user_id,
                movie_id=f"search-{movie_id}",
                event_type="SEARCHED",
                timestamp=ts + timedelta(seconds=400),
                device_type=device_type,
                session_id=session_id,
                progress_seconds=0,
            )
        )
    )
    return result


def generate_events(payload: GenerateIn) -> dict[str, int]:
    random.seed(42)
    total = 0
    for user_num in range(1, payload.users + 1):
        user_id = f"user-{user_num}"
        device_type = random.choice(sorted(DEVICE_TYPES))
        for session_num in range(1, payload.sessions_per_user + 1):
            movie_id = f"movie-{(user_num + session_num) % 7 + 1}"
            session_id = f"{user_id}-session-{session_num}-{uuid.uuid4().hex[:6]}"
            start_at = utcnow() - timedelta(minutes=random.randint(1, 60))
            for record in build_demo_sequence(user_id, movie_id, device_type, session_id, start_at):
                publish_record(record)
                total += 1
    return {"generated": total}


def metric_day(value: date | None) -> date:
    return value or utcnow().date()


def run_clickhouse_aggregation(target_day: date) -> dict[str, Any]:
    started = time.time()
    day_str = target_day.isoformat()
    client = get_clickhouse_client()
    processed = client.query(
        f"""
        SELECT count()
        FROM analytics.raw_events
        WHERE toDate(timestamp) = toDate('{day_str}')
        """,
    ).result_rows[0][0]
    log.info("aggregation started day=%s rows=%s", day_str, processed)

    client.command(
        f"""
        INSERT INTO analytics.daily_metrics
        SELECT
            toDate('{day_str}') AS metric_date,
            'dau' AS metric_name,
            '' AS dimension,
            toFloat64(uniqExact(user_id)) AS value,
            now('UTC') AS calculated_at
        FROM analytics.raw_events
        WHERE toDate(timestamp) = toDate('{day_str}')
        """,
    )
    client.command(
        f"""
        INSERT INTO analytics.daily_metrics
        SELECT
            toDate('{day_str}'),
            'avg_watch_time_seconds',
            '',
            ifNull(avgIf(toFloat64(progress_seconds), event_type = 'VIEW_FINISHED'), 0),
            now('UTC')
        FROM analytics.raw_events
        WHERE toDate(timestamp) = toDate('{day_str}')
        """,
    )
    client.command(
        f"""
        INSERT INTO analytics.daily_metrics
        SELECT
            toDate('{day_str}'),
            'view_conversion',
            '',
            if(
                countIf(event_type = 'VIEW_STARTED') = 0,
                0,
                toFloat64(countIf(event_type = 'VIEW_FINISHED')) / countIf(event_type = 'VIEW_STARTED')
            ),
            now('UTC')
        FROM analytics.raw_events
        WHERE toDate(timestamp) = toDate('{day_str}')
        """,
    )
    client.command(
        f"""
        INSERT INTO analytics.daily_metrics
        SELECT
            toDate('{day_str}'),
            'top_movie_views',
            movie_id,
            toFloat64(views),
            now('UTC')
        FROM
        (
            SELECT
                movie_id,
                countIf(event_type = 'VIEW_FINISHED') AS views,
                row_number() OVER (ORDER BY views DESC, movie_id) AS rn
            FROM analytics.raw_events
            WHERE toDate(timestamp) = toDate('{day_str}')
            GROUP BY movie_id
        )
        WHERE rn <= 5
        """,
    )
    client.command(
        f"""
        INSERT INTO analytics.daily_metrics
        SELECT
            toDate('{day_str}'),
            'device_events',
            device_type,
            toFloat64(count()),
            now('UTC')
        FROM analytics.raw_events
        WHERE toDate(timestamp) = toDate('{day_str}')
        GROUP BY device_type
        """,
    )

    for offset in (1, 7):
        client.command(
            f"""
            INSERT INTO analytics.daily_metrics
            WITH
                toDate('{day_str}') AS cohort_day,
                addDays(cohort_day, {offset}) AS return_day,
                first_seen AS
                (
                    SELECT user_id, min(toDate(timestamp)) AS first_day
                    FROM analytics.raw_events
                    GROUP BY user_id
                ),
                cohort AS
                (
                    SELECT user_id
                    FROM first_seen
                    WHERE first_day = cohort_day
                )
            SELECT
                cohort_day,
                'retention_d{offset}',
                '',
                if(
                    count() = 0,
                    0,
                    toFloat64(countIf(returned = 1)) / count()
                ),
                now('UTC')
            FROM
            (
                SELECT
                    c.user_id,
                    max(if(toDate(r.timestamp) = return_day, 1, 0)) AS returned
                FROM cohort c
                LEFT JOIN analytics.raw_events r ON r.user_id = c.user_id
                GROUP BY c.user_id
            )
            """,
        )

    for offset in range(8):
        client.command(
            f"""
            INSERT INTO analytics.retention_cohort
            WITH
                toDate('{day_str}') AS cohort_day,
                addDays(cohort_day, {offset}) AS return_day,
                first_seen AS
                (
                    SELECT user_id, min(toDate(timestamp)) AS first_day
                    FROM analytics.raw_events
                    GROUP BY user_id
                ),
                cohort AS
                (
                    SELECT user_id
                    FROM first_seen
                    WHERE first_day = cohort_day
                )
            SELECT
                cohort_day,
                {offset},
                if(
                    count() = 0,
                    0,
                    toFloat64(countIf(returned = 1)) / count()
                ),
                count(),
                now('UTC')
            FROM
            (
                SELECT
                    c.user_id,
                    max(if(toDate(r.timestamp) = return_day, 1, 0)) AS returned
                FROM cohort c
                LEFT JOIN analytics.raw_events r ON r.user_id = c.user_id
                GROUP BY c.user_id
            )
            """,
        )

    elapsed = round(time.time() - started, 3)
    result = {"day": day_str, "processed_rows": int(processed), "elapsed_seconds": elapsed}
    log.info("aggregation finished day=%s rows=%s elapsed=%s", day_str, processed, elapsed)
    client.close()
    return result


def upsert_postgres(target_day: date) -> int:
    day_str = target_day.isoformat()
    client = get_clickhouse_client()
    rows = client.query(
        f"""
        SELECT metric_date, metric_name, dimension, argMax(value, calculated_at) AS value, max(calculated_at) AS latest_calculated_at
        FROM analytics.daily_metrics
        WHERE metric_date = toDate('{day_str}')
        GROUP BY metric_date, metric_name, dimension
        UNION ALL
        SELECT cohort_date, 'retention_cohort', concat('day_', toString(day_number)), argMax(retention, calculated_at), max(calculated_at) AS latest_calculated_at
        FROM analytics.retention_cohort
        WHERE cohort_date = toDate('{day_str}')
        GROUP BY cohort_date, day_number
        """,
    ).result_rows
    client.close()
    if not rows:
        return 0

    last_error = None
    for attempt in range(5):
        try:
            with psycopg.connect(settings.postgres_dsn) as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO aggregate_results (metric_date, metric_name, dimension, value, calculated_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (metric_date, metric_name, dimension)
                        DO UPDATE SET
                            value = EXCLUDED.value,
                            calculated_at = EXCLUDED.calculated_at
                        """,
                        [
                            (
                                row[0],
                                row[1],
                                row[2],
                                row[3],
                                row[4].replace(tzinfo=UTC) if getattr(row[4], "tzinfo", None) is None else row[4].astimezone(UTC),
                            )
                            for row in rows
                        ],
                    )
                conn.commit()
            return len(rows)
        except Exception as exc:
            last_error = exc
            time.sleep(2**attempt)
            log.warning("postgres upsert retry=%s error=%s", attempt + 1, exc)
    raise RuntimeError(f"failed to write aggregates to postgres: {last_error}")


def export_day(target_day: date) -> dict[str, Any]:
    day_str = target_day.isoformat()
    with psycopg.connect(settings.postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT metric_date, metric_name, dimension, value, calculated_at
                FROM aggregate_results
                WHERE metric_date = %s
                ORDER BY metric_name, dimension
                """,
                (day_str,),
            )
            rows = cur.fetchall()

    payload = [
        {
            "metric_date": row[0].isoformat(),
            "metric_name": row[1],
            "dimension": row[2],
            "value": float(row[3]),
            "calculated_at": row[4].astimezone(UTC).isoformat(),
        }
        for row in rows
    ]
    key = f"movie-analytics/daily/{day_str}/aggregates.{settings.export_format}"
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode()
    get_s3_client().put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    log.info("export finished day=%s rows=%s key=%s", day_str, len(payload), key)
    return {"day": day_str, "rows": len(payload), "bucket": settings.s3_bucket, "key": key}


def aggregate_and_sync(target_day: date) -> dict[str, Any]:
    result = run_clickhouse_aggregation(target_day)
    result["postgres_rows"] = upsert_postgres(target_day)
    return result


def scheduler_loop() -> None:
    while True:
        target_day = utcnow().date() - timedelta(days=settings.scheduler_target_lag_days)
        try:
            aggregate_and_sync(target_day)
            export_day(target_day)
        except Exception as exc:
            log.exception("scheduled aggregation failed: %s", exc)
        time.sleep(settings.aggregation_interval_seconds)


def bootstrap_background() -> None:
    try:
        bootstrap_once()
    except Exception as exc:
        log.exception("background bootstrap failed: %s", exc)


def scheduler_background() -> None:
    bootstrap_once()
    scheduler_loop()


@app.on_event("startup")
def startup() -> None:
    global SCHEDULER_STARTED
    threading.Thread(target=bootstrap_background, daemon=True).start()
    if settings.app_role == "aggregator" and not SCHEDULER_STARTED:
        threading.Thread(target=scheduler_background, daemon=True).start()
        SCHEDULER_STARTED = True
        log.info("scheduler started interval=%s", settings.aggregation_interval_seconds)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "role": settings.app_role,
        "bootstrapped": BOOTSTRAPPED,
        "scheduler_started": SCHEDULER_STARTED,
    }


@app.post("/events")
def publish_event(event: EventIn) -> dict[str, Any]:
    bootstrap_once()
    return publish_record(to_record(event))


@app.post("/generate")
def generate(payload: GenerateIn) -> dict[str, int]:
    bootstrap_once()
    return generate_events(payload)


@app.post("/aggregate")
def aggregate(payload: DayIn) -> dict[str, Any]:
    bootstrap_once()
    return aggregate_and_sync(metric_day(payload.day))


@app.post("/export")
def export(payload: DayIn) -> dict[str, Any]:
    bootstrap_once()
    return export_day(metric_day(payload.day))
