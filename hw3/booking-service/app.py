from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import grpc
from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from db import SessionLocal
from grpc_client import (
    CircuitBreakerOpenError,
    get_flight,
    release_reservation,
    reserve_seats,
    search_flights,
)
from models import Booking
from schemas import BookingCreateRequest, BookingResponse


app = FastAPI(title="Booking Service")


@app.get("/health")
def health():
    return {"status": "ok"}


def grpc_error_to_http(exc: grpc.RpcError):
    code = exc.code()
    detail = exc.details() or "grpc error"

    mapping = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAUTHENTICATED: 401,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 504,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.INTERNAL: 500,
    }
    status_code = mapping.get(code, 500)
    raise HTTPException(status_code=status_code, detail=detail)


@app.get("/flights")
def api_search_flights(
    origin: str = Query(..., min_length=3, max_length=3),
    destination: str = Query(..., min_length=3, max_length=3),
    date: str = Query(...),
):
    try:
        return search_flights(origin.upper(), destination.upper(), date)
    except CircuitBreakerOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except grpc.RpcError as exc:
        grpc_error_to_http(exc)


@app.get("/flights/{flight_id}")
def api_get_flight(flight_id: str):
    try:
        return get_flight(flight_id)
    except CircuitBreakerOpenError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except grpc.RpcError as exc:
        grpc_error_to_http(exc)


@app.post("/bookings", response_model=BookingResponse, status_code=201)
def create_booking(payload: BookingCreateRequest):
    db: Session = SessionLocal()
    try:
        booking_id = uuid4()

        try:
            flight = get_flight(str(payload.flight_id))
            reserve_seats(str(booking_id), str(payload.flight_id), payload.seat_count)
        except CircuitBreakerOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            grpc_error_to_http(exc)

        price_snapshot = Decimal(str(flight["price"]))
        total_price = price_snapshot * payload.seat_count

        booking = Booking(
            id=booking_id,
            user_id=payload.user_id,
            flight_id=payload.flight_id,
            passenger_name=payload.passenger_name,
            passenger_email=payload.passenger_email,
            seat_count=payload.seat_count,
            price_snapshot=price_snapshot,
            total_price=total_price,
            status="CONFIRMED",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        db.add(booking)
        db.commit()
        db.refresh(booking)
        return booking
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"booking creation failed: {exc}")
    finally:
        db.close()


@app.get("/bookings/{booking_id}", response_model=BookingResponse)
def get_booking(booking_id: UUID):
    db: Session = SessionLocal()
    try:
        booking = db.get(Booking, booking_id)
        if not booking:
            raise HTTPException(status_code=404, detail="booking not found")
        return booking
    finally:
        db.close()


@app.post("/bookings/{booking_id}/cancel", response_model=BookingResponse)
def cancel_booking(booking_id: UUID):
    db: Session = SessionLocal()
    try:
        booking = db.get(Booking, booking_id)
        if not booking:
            raise HTTPException(status_code=404, detail="booking not found")

        if booking.status != "CONFIRMED":
            return booking

        try:
            release_reservation(str(booking.id))
        except CircuitBreakerOpenError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        except grpc.RpcError as exc:
            grpc_error_to_http(exc)

        booking.status = "CANCELLED"
        booking.cancelled_at = datetime.now(timezone.utc)
        booking.updated_at = datetime.now(timezone.utc)

        db.commit()
        db.refresh(booking)
        return booking
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"booking cancellation failed: {exc}")
    finally:
        db.close()


@app.get("/bookings", response_model=list[BookingResponse])
def list_bookings(user_id: UUID):
    db: Session = SessionLocal()
    try:
        stmt = (
            select(Booking)
            .where(Booking.user_id == user_id)
            .order_by(Booking.created_at.desc())
        )
        return list(db.execute(stmt).scalars().all())
    finally:
        db.close()