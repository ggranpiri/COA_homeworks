import json
from datetime import datetime
from decimal import Decimal
from typing import Any

from redis.sentinel import Sentinel

from config import (
    REDIS_DB,
    REDIS_MASTER_NAME,
    REDIS_SENTINEL_HOST,
    REDIS_SENTINEL_PORT,
    REDIS_TTL_SECONDS,
)


sentinel = Sentinel(
    [(REDIS_SENTINEL_HOST, REDIS_SENTINEL_PORT)],
    socket_timeout=1,
)

redis_client = sentinel.master_for(
    REDIS_MASTER_NAME,
    socket_timeout=1,
    db=REDIS_DB,
    decode_responses=True,
)


def _json_default(obj: Any):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return str(obj)


def get_json(key: str):
    raw = redis_client.get(key)
    if not raw:
        return None
    return json.loads(raw)


def set_json(key: str, value: dict, ttl: int = REDIS_TTL_SECONDS):
    redis_client.setex(key, ttl, json.dumps(value, default=_json_default))


def delete_key(key: str):
    redis_client.delete(key)


def make_flight_key(flight_id: str) -> str:
    return f"flight:{flight_id}"


def make_search_key(origin: str, destination: str, date: str) -> str:
    return f"search:{origin}:{destination}:{date}"


def delete_search_keys():
    keys = redis_client.keys("search:*")
    if keys:
        redis_client.delete(*keys)