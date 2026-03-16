## Поднять сервис

```shell
  docker-compose up --build
```
## Подключиться к бд
```shell
  docker exec -it hw2-db-1 psql -U app -d marketplace
```
## Прочитать логи
```shell
  docker logs hw2-api-1 --tail 5
```

## Регистрация
### Создание SELLER'а
```shell
  curl -i -X POST http://localhost:8000/auth/register \
      -H "Content-Type: application/json" \         
      -d '{"email":"s@a.ru","password":"123456","role":"SELLER"}'
```

### Сохранение токен SELLER'а в переменную
```shell
  TOK=$(curl -s -X POST http://localhost:8000/auth/login \
      -H "Content-Type: application/json" \
      -d '{"email":"s@a.ru","password":"123456"}')
    
  SELLER=$(echo "$TOK" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
    
  echo "$SELLER" | head -c 40; echo
```
### Сохранение токен USER'а в переменную
```shell
  TOK=$(curl -s -X POST http://localhost:8000/auth/login \
      -H "Content-Type: application/json" \
      -d '{"email":"buyer@a.ru","password":"123456"}')
    
  USER=$(echo "$TOK" | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")
    
  echo "$USER" | head -c 40; echo
```

## Создание продукта
### Создание продукта
```shell
  curl -s -X POST http://localhost:8000/products \
      -H "Authorization: Bearer $SELLER" \
      -H "Content-Type: application/json" \
      -d '{"name":"Apple Watch","description":"S9","price":199.99,"stock":5,"category":"gadgets","status":"ACTIVE"}'
```
- в `products` создался продукт

### Список всех продуктов
```shell
  curl -s "http://localhost:8000/products?page=0&size=20&status=ACTIVE&category=gadgets"
```

### Мягкое удаление
```shell
  curl -i -X DELETE http://localhost:8000/products/1 -H "Authorization: Bearer $SELLER"
```
- в `products` изменился статус на ARCHIVED

## Цикл заказа
### Создание заказа
```shell
  curl -i -X POST http://localhost:8000/orders \
      -H "Authorization: Bearer $USER" \
      -H "Content-Type: application/json" \
      -d '{
            "items": [
              { "product_id": 2, "quantity": 2 }
            ],
            "promo_code": "SALE10"
          }'
```
- в `products` уменьшился запас (stock)
- в `orders` и `order_items` создался заказ
- в `user_operations` добавилась операция с `operation_type=CREATE_ORDER`

### Создание второго заказа у того же пользователя
```shell
  curl -i -X POST http://localhost:8000/orders \
      -H "Authorization: Bearer $USER" \
      -H "Content-Type: application/json" \
      -d '{
            "items": [
              { "product_id": 2, "quantity": 1 }
            ]
          }'
```
{"error_code":"ORDER_HAS_ACTIVE","message":"User already has active order","details":{}}%                                                                                            



### Обновление заказа
```shell
  curl -i -X PUT http://localhost:8000/orders/2 \
      -H "Authorization: Bearer $USER" \
      -H "Content-Type: application/json" \
      -d '{
            "items": [
              { "product_id": 2, "quantity": 1 }
            ]
          }'
```
- в ```products``` обновился запас (stock)
- в ```orders``` и ```order_items``` обновился заказ
- в ```user_operations``` добавилась операция с ```operation_type=UPDATE_ORDER```

### Отмена заказа
```shell
  curl -i -X POST http://localhost:8000/orders/2/cancel \
      -H "Authorization: Bearer $USER"
```
- в ```orders``` обновился статус заказа

### Создание слишком большого заказа
```shell
  curl -i -X POST http://localhost:8000/orders \
      -H "Authorization: Bearer $USER" \
      -H "Content-Type: application/json" \
      -d '{
            "items": [
              { "product_id": 2, "quantity": 10000 }
            ]
          }'
```
{"error_code":"VALIDATION_ERROR","message":"Validation failed","details":{"fields":[{"field":"items.0.quantity","message":"Input should be less than or equal to 999"}]}}%

## Создание промокода
```shell
  curl -i -X POST http://localhost:8000/promo-codes \
      -H "Authorization: Bearer $SELLER" \
      -H "Content-Type: application/json" \
      -d '{
        "code":"SALE10",
        "discount_type":"PERCENTAGE",
        "discount_value":10,
        "min_order_amount":500,
        "max_uses":2,
        "valid_from":"2026-01-01T00:00:00Z",
        "valid_until":"2026-12-31T23:59:59Z",
        "active":true
      }'
```

