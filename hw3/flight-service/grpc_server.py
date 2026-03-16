from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from auth import check_api_key
from db import SessionLocal
from models import Flight, SeatReservation
from redis_cache import (
    delete_key,
    delete_search_keys,
    get_json,
    make_flight_key,
    make_search_key,
    set_json,
)
from proto import flight_pb2, flight_pb2_grpc


def dt_to_ts(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt.astimezone(timezone.utc))
    return ts


def flight_to_proto(flight: Flight) -> flight_pb2.FlightDto:
    status_map = {
        "SCHEDULED": flight_pb2.SCHEDULED,
        "DEPARTED": flight_pb2.DEPARTED,
        "CANCELLED": flight_pb2.CANCELLED,
        "COMPLETED": flight_pb2.COMPLETED,
    }
    return flight_pb2.FlightDto(
        id=str(flight.id),
        flight_number=flight.flight_number,
        airline=flight.airline,
        origin_iata=flight.origin_iata,
        destination_iata=flight.destination_iata,
        departure_time=dt_to_ts(flight.departure_time),
        arrival_time=dt_to_ts(flight.arrival_time),
        total_seats=flight.total_seats,
        available_seats=flight.available_seats,
        price=float(flight.price),
        status=status_map.get(flight.status, flight_pb2.FLIGHT_STATUS_UNSPECIFIED),
    )


class FlightServiceServicer(flight_pb2_grpc.FlightServiceServicer):
    def SearchFlights(self, request, context):
        if not check_api_key(context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")

        if not request.origin or not request.destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "origin and destination are required")

        if not request.HasField("date"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "date is required")

        search_dt = request.date.ToDatetime().astimezone(timezone.utc)
        search_date = search_dt.date().isoformat()

        cache_key = make_search_key(request.origin, request.destination, search_date)
        cached = get_json(cache_key)
        if cached:
            print(f"CACHE HIT search:{request.origin}:{request.destination}:{search_date}", flush=True)
        else:
            print(f"CACHE MISS search:{request.origin}:{request.destination}:{search_date}", flush=True)

        if cached:
            flights = []
            for item in cached["flights"]:
                dep = Timestamp()
                dep.FromJsonString(item["departure_time"])
                arr = Timestamp()
                arr.FromJsonString(item["arrival_time"])

                flights.append(
                    flight_pb2.FlightDto(
                        id=item["id"],
                        flight_number=item["flight_number"],
                        airline=item["airline"],
                        origin_iata=item["origin_iata"],
                        destination_iata=item["destination_iata"],
                        departure_time=dep,
                        arrival_time=arr,
                        total_seats=item["total_seats"],
                        available_seats=item["available_seats"],
                        price=item["price"],
                        status=item["status"],
                    )
                )
            return flight_pb2.SearchFlightsResponse(flights=flights)

        db: Session = SessionLocal()
        try:
            stmt = (
                select(Flight)
                .where(Flight.origin_iata == request.origin)
                .where(Flight.destination_iata == request.destination)
                .where(Flight.status == "SCHEDULED")
            )
            flights = db.execute(stmt).scalars().all()

            filtered = []
            payload = {"flights": []}

            for flight in flights:
                if flight.departure_time.date().isoformat() != search_date:
                    continue
                proto_flight = flight_to_proto(flight)
                filtered.append(proto_flight)
                payload["flights"].append(
                    {
                        "id": str(flight.id),
                        "flight_number": flight.flight_number,
                        "airline": flight.airline,
                        "origin_iata": flight.origin_iata,
                        "destination_iata": flight.destination_iata,
                        "departure_time": flight.departure_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "arrival_time": flight.arrival_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "total_seats": flight.total_seats,
                        "available_seats": flight.available_seats,
                        "price": float(flight.price),
                        "status": proto_flight.status,
                    }
                )

            set_json(cache_key, payload)
            return flight_pb2.SearchFlightsResponse(flights=filtered)
        finally:
            db.close()

    def GetFlight(self, request, context):
        if not check_api_key(context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")

        if not request.id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "flight id is required")

        cache_key = make_flight_key(request.id)
        cached = get_json(cache_key)

        if cached:
            print(f"CACHE HIT {cache_key}", flush=True)
        else:
            print(f"CACHE MISS {cache_key}", flush=True)
        if cached:
            dep = Timestamp()
            dep.FromJsonString(cached["departure_time"])
            arr = Timestamp()
            arr.FromJsonString(cached["arrival_time"])

            return flight_pb2.FlightResponse(
                flight=flight_pb2.FlightDto(
                    id=cached["id"],
                    flight_number=cached["flight_number"],
                    airline=cached["airline"],
                    origin_iata=cached["origin_iata"],
                    destination_iata=cached["destination_iata"],
                    departure_time=dep,
                    arrival_time=arr,
                    total_seats=cached["total_seats"],
                    available_seats=cached["available_seats"],
                    price=cached["price"],
                    status=cached["status"],
                )
            )

        db: Session = SessionLocal()
        try:
            try:
                flight_id = UUID(request.id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight id")

            flight = db.get(Flight, flight_id)
            if not flight:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")

            proto_flight = flight_to_proto(flight)

            set_json(
                cache_key,
                {
                    "id": str(flight.id),
                    "flight_number": flight.flight_number,
                    "airline": flight.airline,
                    "origin_iata": flight.origin_iata,
                    "destination_iata": flight.destination_iata,
                    "departure_time": flight.departure_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "arrival_time": flight.arrival_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "total_seats": flight.total_seats,
                    "available_seats": flight.available_seats,
                    "price": float(flight.price),
                    "status": proto_flight.status,
                },
            )

            return flight_pb2.FlightResponse(flight=proto_flight)
        finally:
            db.close()

    def ReserveSeats(self, request, context):
        if not check_api_key(context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")

        if not request.booking_id or not request.flight_id or request.seat_count <= 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "booking_id, flight_id and positive seat_count are required",
            )

        db: Session = SessionLocal()
        try:
            try:
                booking_id = UUID(request.booking_id)
                flight_id = UUID(request.flight_id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid uuid")

            existing = db.execute(
                select(SeatReservation).where(SeatReservation.booking_id == booking_id)
            ).scalar_one_or_none()

            if existing:
                status_map = {
                    "ACTIVE": flight_pb2.ACTIVE,
                    "RELEASED": flight_pb2.RELEASED,
                    "EXPIRED": flight_pb2.EXPIRED,
                }
                return flight_pb2.ReserveSeatsResponse(
                    reservation_id=str(existing.id),
                    status=status_map.get(existing.status, flight_pb2.RESERVATION_STATUS_UNSPECIFIED),
                )

            flight = db.execute(
                select(Flight).where(Flight.id == flight_id).with_for_update()
            ).scalar_one_or_none()

            if not flight:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")

            if flight.status != "SCHEDULED":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "flight is not available for booking")

            if flight.available_seats < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough seats")

            flight.available_seats -= request.seat_count

            reservation = SeatReservation(
                id=uuid4(),
                booking_id=booking_id,
                flight_id=flight_id,
                seat_count=request.seat_count,
                status="ACTIVE",
                reserved_at=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )

            db.add(reservation)
            db.commit()

            delete_key(make_flight_key(str(flight_id)))
            delete_search_keys()

            return flight_pb2.ReserveSeatsResponse(
                reservation_id=str(reservation.id),
                status=flight_pb2.ACTIVE,
            )

        except IntegrityError:
            db.rollback()
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "reservation already exists")
        except grpc.RpcError:
            db.rollback()
            raise
        except Exception as e:
            db.rollback()
            context.abort(grpc.StatusCode.INTERNAL, f"reserve failed: {e}")
        finally:
            db.close()

    def ReleaseReservation(self, request, context):
        if not check_api_key(context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid api key")

        if not request.booking_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "booking_id is required")

        db: Session = SessionLocal()
        try:
            try:
                booking_id = UUID(request.booking_id)
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid booking_id")

            reservation = db.execute(
                select(SeatReservation)
                .where(SeatReservation.booking_id == booking_id)
                .with_for_update()
            ).scalar_one_or_none()

            if not reservation:
                context.abort(grpc.StatusCode.NOT_FOUND, "reservation not found")

            if reservation.status != "ACTIVE":
                status_map = {
                    "ACTIVE": flight_pb2.ACTIVE,
                    "RELEASED": flight_pb2.RELEASED,
                    "EXPIRED": flight_pb2.EXPIRED,
                }
                return flight_pb2.ReleaseReservationResponse(
                    reservation_id=str(reservation.id),
                    status=status_map.get(reservation.status, flight_pb2.RESERVATION_STATUS_UNSPECIFIED),
                )

            flight = db.execute(
                select(Flight).where(Flight.id == reservation.flight_id).with_for_update()
            ).scalar_one_or_none()

            if not flight:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")

            flight.available_seats += reservation.seat_count
            reservation.status = "RELEASED"
            reservation.released_at = datetime.now(timezone.utc)
            reservation.updated_at = datetime.now(timezone.utc)

            db.commit()

            delete_key(make_flight_key(str(flight.id)))
            delete_search_keys()

            return flight_pb2.ReleaseReservationResponse(
                reservation_id=str(reservation.id),
                status=flight_pb2.RELEASED,
            )

        except grpc.RpcError:
            db.rollback()
            raise
        except Exception as e:
            db.rollback()
            context.abort(grpc.StatusCode.INTERNAL, f"release failed: {e}")
        finally:
            db.close()