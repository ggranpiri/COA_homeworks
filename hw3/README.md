
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

Получение бронирования
```shell
curl "http://localhost:8003/bookings/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
```

Список бронирований пользователя
```shell
curl "http://localhost:8003/bookings?user_id=cccccccc-cccc-cccc-cccc-cccccccccccc"
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
order by created_at desc;
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

Отчистка кэша: `docker exec -it flight-redis redis-cli flushall  `

```shell
curl "http://localhost:8003/flights?origin=SVO&destination=LED&date=2026-03-20T00:00:00"
```

Ключи создаются: `docker exec -it redis-master redis-cli keys "*"`

В логах `docker compose logs -f flight-service ` 
- при добавлении: `CACHE MISS search:SVO:LED:2026-03-20`
- при чтении: `CACHE HIT search:SVO:LED:2026-03-20`

TTL есть: `docker exec -it redis-master redis-cli ttl search:SVO:LED:2026-03-20`


## 8. Retry при вызовах Flight Service 
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

## 9. Redis в кластерном режиме
Поднялись 3 контейнера: `docker ps`

Sentinel знает мастер: `docker exec -it redis-sentinel redis-cli -p 26379 SENTINEL get-master-addr-by-name mymaster`

1) "redis-master"
2) "6379"
   
Мастер: `docker exec -it redis-replica redis-cli info replication`

Реплика: `docker exec -it redis-master redis-cli info replication`

## 10. Circuit Breaker
booking-service/grpc_client.py
```python
import time


class CircuitBreaker:
    def __init__(self):
        self.state = "CLOSED"
        self.failure_count = 0
        self.opened_at = None
        self.half_open_calls = 0

    def before_call(self):
        if self.state == "OPEN":
            if self.opened_at is None:
                print("CIRCUIT BREAKER OPEN -> reject request", flush=True)
                raise CircuitBreakerOpenError("flight service circuit breaker is open")

            if time.time() - self.opened_at >= CB_RECOVERY_TIMEOUT:
                print("CIRCUIT BREAKER OPEN -> HALF_OPEN", flush=True)
                self.state = "HALF_OPEN"
                self.half_open_calls = 0
            else:
                print("CIRCUIT BREAKER OPEN -> reject request", flush=True)
                raise CircuitBreakerOpenError("flight service circuit breaker is open")

        if self.state == "HALF_OPEN":
            if self.half_open_calls >= CB_HALF_OPEN_MAX_CALLS:
                print("CIRCUIT BREAKER HALF_OPEN -> reject extra probe", flush=True)
                raise CircuitBreakerOpenError("flight service circuit breaker is half-open")

            self.half_open_calls += 1
            print(f"CIRCUIT BREAKER HALF_OPEN -> allow probe #{self.half_open_calls}", flush=True)

    def record_success(self):
        if self.state != "CLOSED":
            print(f"CIRCUIT BREAKER {self.state} -> CLOSED", flush=True)

        self.state = "CLOSED"
        self.failure_count = 0
        self.opened_at = None
        self.half_open_calls = 0

    def record_failure(self):
        if self.state == "HALF_OPEN":
            print("CIRCUIT BREAKER HALF_OPEN -> OPEN", flush=True)
            self.state = "OPEN"
            self.opened_at = time.time()
            self.half_open_calls = 0
            return

        self.failure_count += 1
        print(
            f"CIRCUIT BREAKER CLOSED failure_count={self.failure_count}/{CB_FAILURE_THRESHOLD}",
            flush=True,
        )

        if self.failure_count >= CB_FAILURE_THRESHOLD:
            print("CIRCUIT BREAKER CLOSED -> OPEN", flush=True)
            self.state = "OPEN"
            self.opened_at = time.time()

```