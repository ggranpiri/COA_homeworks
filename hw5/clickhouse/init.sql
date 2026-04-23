CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.raw_events
(
    event_id String,
    user_id String,
    movie_id String,
    event_type LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    device_type LowCardinality(String),
    session_id String,
    progress_seconds UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, session_id, timestamp, event_id);

CREATE TABLE IF NOT EXISTS analytics.movie_events_queue
(
    event_id String,
    user_id String,
    movie_id String,
    event_type String,
    timestamp DateTime64(3, 'UTC'),
    device_type String,
    session_id String,
    progress_seconds UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events',
    kafka_format = 'AvroConfluent',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_thread_per_consumer = 0,
    format_avro_schema_registry_url = 'http://schema-registry:8081';

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.movie_events_mv
TO analytics.raw_events
AS
SELECT *
FROM analytics.movie_events_queue;

CREATE TABLE IF NOT EXISTS analytics.daily_metrics
(
    metric_date Date,
    metric_name LowCardinality(String),
    dimension String,
    value Float64,
    calculated_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, metric_name, dimension);

CREATE TABLE IF NOT EXISTS analytics.retention_cohort
(
    cohort_date Date,
    day_number UInt8,
    retention Float64,
    cohort_size UInt64,
    calculated_at DateTime('UTC')
)
ENGINE = ReplacingMergeTree(calculated_at)
PARTITION BY toYYYYMM(cohort_date)
ORDER BY (cohort_date, day_number);
