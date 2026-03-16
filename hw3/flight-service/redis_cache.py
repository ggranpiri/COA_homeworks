import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import redis

from config import REDIS_TTL_SECONDS, REDIS_URL


redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)


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