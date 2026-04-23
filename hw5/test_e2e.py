import json
import base64
import time
import unittest
import urllib.parse
import urllib.request
from datetime import UTC, datetime


def http_json(method: str, url: str, payload: dict | None = None) -> dict:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode())


def wait_json(url: str, timeout: int = 180) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return http_json("GET", url)
        except Exception:
            time.sleep(2)
    raise TimeoutError(f"timeout waiting for {url}")


def clickhouse_scalar(sql: str, timeout: int = 60) -> str:
    query = urllib.parse.quote(sql)
    request = urllib.request.Request(
        f"http://localhost:8123/?database=analytics&query={query}",
        headers={
            "Authorization": "Basic " + base64.b64encode(b"default:").decode()
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode().strip()


class PipelineTest(unittest.TestCase):
    def test_full_pipeline(self) -> None:
        wait_json("http://localhost:8000/health")
        wait_json("http://localhost:8001/health")

        today = datetime.now(UTC).date().isoformat()
        session_id = f"test-session-{int(time.time())}"
        user_id = f"test-user-{int(time.time())}"

        payloads = [
            {
                "user_id": user_id,
                "movie_id": "movie-42",
                "event_type": "VIEW_STARTED",
                "device_type": "DESKTOP",
                "session_id": session_id,
                "progress_seconds": 0,
            },
            {
                "user_id": user_id,
                "movie_id": "movie-42",
                "event_type": "VIEW_FINISHED",
                "device_type": "DESKTOP",
                "session_id": session_id,
                "progress_seconds": 480,
            },
        ]

        event_ids = []
        for payload in payloads:
            response = http_json("POST", "http://localhost:8000/events", payload)
            event_ids.append(response["event_id"])

        deadline = time.time() + 120
        while time.time() < deadline:
            count = int(
                clickhouse_scalar(
                    "SELECT count() FROM raw_events "
                    f"WHERE event_id IN ({','.join(repr(item) for item in event_ids)}) FORMAT TabSeparatedRaw"
                )
            )
            if count == 2:
                break
            time.sleep(2)
        else:
            self.fail("events did not reach ClickHouse")

        aggregate = http_json("POST", "http://localhost:8001/aggregate", {"day": today})
        self.assertGreaterEqual(aggregate["processed_rows"], 2)
        self.assertGreaterEqual(aggregate["postgres_rows"], 1)

        daily_metrics = int(
            clickhouse_scalar(
                f"SELECT count() FROM daily_metrics WHERE metric_date = toDate('{today}') FORMAT TabSeparatedRaw"
            )
        )
        self.assertGreaterEqual(daily_metrics, 3)

        export = http_json("POST", "http://localhost:8001/export", {"day": today})
        self.assertGreaterEqual(export["rows"], 1)
        self.assertTrue(export["key"].endswith(".json"))


if __name__ == "__main__":
    unittest.main()
