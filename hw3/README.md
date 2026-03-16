
Генерация proto-файлов для flight-service

```shell
python -m grpc_tools.protoc \
  -I./proto \
  --python_out=./flight_service \
  --grpc_python_out=./flight_service \
  ./proto/flight.proto
```

Подключиться в БД booking-service
```shell
docker exec -it booking-db psql -U booking_user -d booking_db
```

Подключиться в БД flight-service
```shell
docker exec -it flight-db psql -U flight_user -d flight_db
```



Поиск рейсов
```shell
curl "http://localhost:8003/flights?origin=SVO&destination=LED&date=2026-03-20T00:00:00"
```

Получение рейса
```shell
curl "http://localhost:8003/flights/11111111-1111-1111-1111-111111111111"
```

Создание бронирования
```shell
curl -X POST http://localhost:8003/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":"cccccccc-cccc-cccc-cccc-cccccccccccc",
    "flight_id":"11111111-1111-1111-1111-111111111111",
    "passenger_name":"Sofia Sovkova",
    "passenger_email":"sofia@example.com",
    "seat_count":2
  }'
```

Проверка готового бронирования из seed
```shell
curl "http://localhost:8003/bookings/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
```

Список бронирований пользователя
```shell
curl "http://localhost:8003/bookings?user_id=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
```
Отмена бронирования
```shell
curl -X POST http://localhost:8003/bookings/6d6941db-dac7-46d9-a46c-e858b261ba81/cancel
```

## 5. Транзакционная целостность
fights (остатки мест)
```sql
select id, flight_number, available_seats from flights;
```
flights (резервации)
```sql
select id, booking_id, flight_id, seat_count, status
from seat_reservations
order by created_at desc
```
booking (бронирования)
```sql
select id, user_id, flight_id, status
from bookings
order by created_at desc;
```

### Создание бронирования
```shell
curl -X POST http://localhost:8003/bookings \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":"cccccccc-cccc-cccc-cccc-cccccccccccc",
    "flight_id":"11111111-1111-1111-1111-111111111111",
    "passenger_name":"Sofia Sovkova",
    "passenger_email":"sofia@example.com",
    "seat_count":2
  }'
```
### Отмена бронирования
```shell
curl -X POST http://localhost:8003/bookings/6d6941db-dac7-46d9-a46c-e858b261ba81/cancel
```


## 6. Аутентификация межсервисных вызовов
Сломаем GRPC_API_KEY у booking-service:

`GRPC_API_KEY = os.getenv("GRPC_API_KEY3")`

Выполним запрос:
```shell
curl "http://localhost:8003/flights/11111111-1111-1111-1111-111111111111"
```
Получим: {"detail":"invalid api key"}

## 7. Redis для кеширования

Redis жив: `docker exec -it flight-redis redis-cli ping`

Отчистка логов `docker exec -it flight-redis redis-cli flushall  `

```shell
curl "http://localhost:8003/flights?origin=SVO&destination=LED&date=2026-03-20T00:00:00"
```

Ключи создаются: `docker exec -it flight-redis redis-cli keys "*"`

В логах `docker compose logs -f flight-service ` 
- при добавлении: `CACHE MISS search:SVO:LED:2026-03-20`
- при чтении: `CACHE HIT search:SVO:LED:2026-03-20`

TTL есть: `docker exec -it flight-redis redis-cli ttl search:SVO:LED:2026-03-20`


## 8. 
booking-service/grpc_client.py 

```python
def _is_retryable(exc: grpc.RpcError) -> bool:
    return exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED) # <- Retry только для: UNAVAILABLE, DEADLINE_EXCEEDED


def _call_with_retry(call_fn):
    backoffs = [GRPC_BACKOFF_1, GRPC_BACKOFF_2, GRPC_BACKOFF_3] # <- Exponential backoff (например: 100ms, 200ms, 400ms)

    breaker.before_call()

    last_exc = None
    attempts = max(1, GRPC_RETRY_ATTEMPTS) # <= Максимум 3 попытки.

    for attempt in range(attempts):
        print(f"Retry attempt {attempt+1}", flush=True)
        try:
            result = call_fn()
            breaker.record_success()
            return result
        except grpc.RpcError as exc:
            last_exc = exc
            if _is_retryable(exc):
                breaker.record_failure()
                if attempt < attempts - 1:
                    sleep_for = backoffs[min(attempt, len(backoffs) - 1)]
                    time.sleep(sleep_for)
                    continue
            raise

    if last_exc:
        raise last_exc
```

Останавливаем flight-service: `docker stop flight-service`

Делаем запрос
```shell
curl "http://localhost:8003/flights/11111111-1111-1111-1111-111111111111"
```

В логах `docker compose logs -f booking-service` видим 3 попытки и только после ошибку:
```log
booking-service  | Retry attempt 1
booking-service  | Retry attempt 2
booking-service  | Retry attempt 3
booking-service  | INFO:     192.168.65.1:61651 - "GET /flights/11111111-1111-1111-1111-111111111111 HTTP/1.1" 503 Service Unavailable
```
