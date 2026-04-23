# HW5

Online Cinema: Event Streaming + Analytics Pipeline

Стек: `Kafka + Schema Registry + ClickHouse + PostgreSQL + MinIO + Grafana + Python(FastAPI)`.

## Структура

- `service/` — один Python-сервис для producer, генератора, агрегации и экспорта
- `clickhouse/` — init SQL для ClickHouse
- `postgres/` — init SQL для PostgreSQL
- `grafana/` — datasource и dashboard provisioning
- `docker-compose.yml` — вся инфраструктура
- `test_e2e.py` — интеграционный тест

## Запуск

```shell
docker compose up --build -d
```

## Подключение к БД

Подключиться в PostgreSQL:

```shell
docker exec -it hw5-postgres psql -U app -d analytics
```

Подключиться в ClickHouse:

```shell
docker exec -it hw5-clickhouse clickhouse-client --database analytics
```

## Логи

Логи producer:

```shell
docker logs hw5-producer --tail 200
```

Логи aggregator:

```shell
docker logs hw5-aggregator --tail 200
```


## 1. Kafka topic со схемой событий

Cхема зарегистрирована в Schema Registry:

```shell
docker exec -it hw5-schema-registry curl -s http://localhost:8081/subjects
```

Последняя версия схемы:

```shell
docker exec -it hw5-schema-registry curl -s http://localhost:8081/subjects/movie-events-value/versions/latest
```

Topic создан и у него 3 partition:

```shell
docker exec -it hw5-kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic movie-events
```

`user_id` выбран, чтобы сохранить порядок событий одного пользователя в Kafka. Для сценариев `VIEW_STARTED -> VIEW_PAUSED -> VIEW_RESUMED -> VIEW_FINISHED` это важнее, чем распределение по `movie_id`.

## 2. Продюсер

Проверка health:

```shell
curl http://localhost:8000/health
```

Отправить одно событие:

```shell
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":"user-1",
    "movie_id":"movie-7",
    "event_type":"VIEW_STARTED",
    "device_type":"DESKTOP",
    "session_id":"session-1",
    "progress_seconds":0
  }'
```
{"event_id":"63557400-f35a-4de9-a79e-eb150feaaa4c","partition":2,"offset":28}% 

Сгенерировать поток синтетических событий:

```shell
curl -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{
    "users": 5,
    "sessions_per_user": 2
  }'
```
{"generated":60}%   


## 3. ClickHouse: приём и хранение событий

raw-события попадают в ClickHouse:

```sql
select event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
from raw_events
order by timestamp desc
limit 20;
```

Kafka Engine и materialized view созданы:

```sql
show tables;
```

- есть `movie_events_queue`
- есть `movie_events_mv`
- есть `raw_events`


## 4. Интеграционный тест pipeline


```shell
python3 -m unittest -q test_e2e.py
```

Тест проверяет:

- публикацию события через producer
- появление события в ClickHouse
- пересчёт агрегатов
- экспорт результата

## 5. Сервис агрегации и выгрузка в PostgreSQL
Ручной запуск агрегации за дату:

```shell
curl -X POST http://localhost:8001/aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "day":"2026-04-23"
  }'
```

{"day":"2026-04-23","processed_rows":67,"elapsed_seconds":0.121,"postgres_rows":21}% 

Агрегаты в ClickHouse:

```sql
select metric_date, metric_name, dimension, value, calculated_at
from daily_metrics
order by metric_date desc, metric_name, dimension
limit 50;
```

retention cohort:

```sql
select cohort_date, day_number, retention, cohort_size, calculated_at
from retention_cohort
order by cohort_date desc, day_number asc
limit 50;
```

агрегаты в PostgreSQL:

```sql
select metric_date, metric_name, dimension, value, calculated_at
from aggregate_results
order by calculated_at desc
limit 50;
```


Проверка идемпотентности:

```shell
curl -X POST http://localhost:8001/aggregate \
  -H "Content-Type: application/json" \
  -d '{"day":"2026-04-23"}'
```

Повторить ту же команду ещё раз и затем проверить:

```sql
select metric_date, metric_name, dimension, count(*)
from aggregate_results
group by metric_date, metric_name, dimension
having count(*) > 1;
```

 metric_date | metric_name | dimension | count 
-------------+-------------+-----------+-------
(0 rows)

## 6. Grafana-dashboard

Открыть Grafana:

```text
http://localhost:3000
```

- datasource `ClickHouse` создан автоматически
- dashboard `Movie Analytics` создан автоматически
- есть панель `Retention Cohort Heatmap`
- есть дополнительные панели `DAU`, `View Conversion`, `Top Movies`, `Average Watch Time`


## 7. Экспорт в S3

Ручной запуск экспорта:

```shell
curl -X POST http://localhost:8001/export \
  -H "Content-Type: application/json" \
  -d '{
    "day":"2026-04-23"
  }'
```
{"day":"2026-04-23","rows":22,"bucket":"movie-analytics","key":"movie-analytics/daily/2026-04-23/aggregates.json"}%   

bucket существует:

```shell
docker exec -it hw5-minio mc alias set local http://127.0.0.1:9000 minio minio123
docker exec -it hw5-minio mc ls local
```

Файл появился:

```shell
docker exec -it hw5-minio mc ls local/movie-analytics --recursive
```

[2026-04-23 10:15:56 UTC] 3.7KiB STANDARD movie-analytics/daily/2026-04-23/aggregates.json

Просмотреть файл:
```shell
docker exec -it hw5-minio sh -lc "mc alias set local http://127.0.0.1:9000 minio minio123 >/dev/null && mc cat local/movie-analytics/movie-analytics/daily/2026-04-23/aggregates.json"
```


Повторный экспорт за ту же дату:

```shell
curl -X POST http://localhost:8001/export \
  -H "Content-Type: application/json" \
  -d '{"day":"2026-04-23"}'
```

- путь не меняется
- файл перезаписывается, а не плодит дубликаты

## 8. Отказоустойчивая Kafka-инфраструктура

Брокеров два:

```shell
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}'
```


```shell
docker exec -it hw5-kafka-1 kafka-topics --bootstrap-server kafka-1:9092 --describe --topic movie-events
```

- `hw5-kafka-1` и `hw5-kafka-2` запущены
- `ReplicationFactor:2`
- `min.insync.replicas=1`
- `schema-registry` поднят
- у сервисов есть healthcheck и `docker compose ps` показывает `healthy`


