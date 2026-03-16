import time
from datetime import datetime, timezone

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from auth import grpc_metadata
from config import (
    CB_FAILURE_THRESHOLD,
    CB_HALF_OPEN_MAX_CALLS,
    CB_RECOVERY_TIMEOUT,
    FLIGHT_SERVICE_HOST,
    FLIGHT_SERVICE_PORT,
    GRPC_BACKOFF_1,
    GRPC_BACKOFF_2,
    GRPC_BACKOFF_3,
    GRPC_RETRY_ATTEMPTS,
    GRPC_TIMEOUT_SECONDS,
)
from proto import flight_pb2, flight_pb2_grpc


class CircuitBreakerOpenError(Exception):
    pass


class CircuitBreaker:
    def __init__(self):
        self.state = "CLOSED"
        self.failure_count = 0
        self.opened_at = None
        self.half_open_calls = 0

    def before_call(self):
        if self.state == "OPEN":
            if self.opened_at is None:
                raise CircuitBreakerOpenError("flight service circuit breaker is open")
            if time.time() - self.opened_at >= CB_RECOVERY_TIMEOUT:
                self.state = "HALF_OPEN"
                self.half_open_calls = 0
            else:
                raise CircuitBreakerOpenError("flight service circuit breaker is open")

        if self.state == "HALF_OPEN":
            if self.half_open_calls >= CB_HALF_OPEN_MAX_CALLS:
                raise CircuitBreakerOpenError("flight service circuit breaker is half-open")
            self.half_open_calls += 1

    def record_success(self):
        self.state = "CLOSED"
        self.failure_count = 0
        self.opened_at = None
        self.half_open_calls = 0

    def record_failure(self):
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            self.opened_at = time.time()
            self.half_open_calls = 0
            return

        self.failure_count += 1
        if self.failure_count >= CB_FAILURE_THRESHOLD:
            self.state = "OPEN"
            self.opened_at = time.time()


breaker = CircuitBreaker()


def _channel():
    target = f"{FLIGHT_SERVICE_HOST}:{FLIGHT_SERVICE_PORT}"
    return grpc.insecure_channel(target)


def _is_retryable(exc: grpc.RpcError) -> bool:
    return exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)


def _call_with_retry(call_fn):
    backoffs = [GRPC_BACKOFF_1, GRPC_BACKOFF_2, GRPC_BACKOFF_3]

    breaker.before_call()

    last_exc = None
    attempts = max(1, GRPC_RETRY_ATTEMPTS)

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


def _status_to_str(status_value: int) -> str:
    mapping = {
        flight_pb2.SCHEDULED: "SCHEDULED",
        flight_pb2.DEPARTED: "DEPARTED",
        flight_pb2.CANCELLED: "CANCELLED",
        flight_pb2.COMPLETED: "COMPLETED",
    }
    return mapping.get(status_value, "FLIGHT_STATUS_UNSPECIFIED")


def _flight_to_dict(flight) -> dict:
    dep = flight.departure_time.ToDatetime().astimezone(timezone.utc)
    arr = flight.arrival_time.ToDatetime().astimezone(timezone.utc)

    return {
        "id": flight.id,
        "flight_number": flight.flight_number,
        "airline": flight.airline,
        "origin_iata": flight.origin_iata,
        "destination_iata": flight.destination_iata,
        "departure_time": dep,
        "arrival_time": arr,
        "total_seats": flight.total_seats,
        "available_seats": flight.available_seats,
        "price": float(flight.price),
        "status": _status_to_str(flight.status),
    }


def search_flights(origin: str, destination: str, date_str: str):
    with _channel() as channel:
        stub = flight_pb2_grpc.FlightServiceStub(channel)

        dt = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        ts = Timestamp()
        ts.FromDatetime(dt)

        request = flight_pb2.SearchFlightsRequest(
            origin=origin,
            destination=destination,
            date=ts,
        )

        response = _call_with_retry(
            lambda: stub.SearchFlights(
                request,
                timeout=GRPC_TIMEOUT_SECONDS,
                metadata=grpc_metadata(),
            )
        )

        return [_flight_to_dict(item) for item in response.flights]


def get_flight(flight_id: str):
    with _channel() as channel:
        stub = flight_pb2_grpc.FlightServiceStub(channel)

        request = flight_pb2.GetFlightRequest(id=flight_id)
        response = _call_with_retry(
            lambda: stub.GetFlight(
                request,
                timeout=GRPC_TIMEOUT_SECONDS,
                metadata=grpc_metadata(),
            )
        )
        return _flight_to_dict(response.flight)


def reserve_seats(booking_id: str, flight_id: str, seat_count: int):
    with _channel() as channel:
        stub = flight_pb2_grpc.FlightServiceStub(channel)

        request = flight_pb2.ReserveSeatsRequest(
            booking_id=booking_id,
            flight_id=flight_id,
            seat_count=seat_count,
        )

        return _call_with_retry(
            lambda: stub.ReserveSeats(
                request,
                timeout=GRPC_TIMEOUT_SECONDS,
                metadata=grpc_metadata(),
            )
        )


def release_reservation(booking_id: str):
    with _channel() as channel:
        stub = flight_pb2_grpc.FlightServiceStub(channel)

        request = flight_pb2.ReleaseReservationRequest(
            booking_id=booking_id,
        )

        return _call_with_retry(
            lambda: stub.ReleaseReservation(
                request,
                timeout=GRPC_TIMEOUT_SECONDS,
                metadata=grpc_metadata(),
            )
        )