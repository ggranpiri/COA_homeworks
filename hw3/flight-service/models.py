from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from db import Base


class Flight(Base):
    __tablename__ = "flights"

    id = Column(UUID(as_uuid=True), primary_key=True)
    flight_number = Column(String(20), nullable=False)
    airline = Column(String(100), nullable=False)

    origin_iata = Column(String(3), nullable=False)
    destination_iata = Column(String(3), nullable=False)

    departure_time = Column(DateTime(timezone=True), nullable=False)
    arrival_time = Column(DateTime(timezone=True), nullable=False)

    total_seats = Column(Integer, nullable=False)
    available_seats = Column(Integer, nullable=False)

    price = Column(Numeric(12, 2), nullable=False)
    status = Column(String(20), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    reservations = relationship("SeatReservation", back_populates="flight")

    __table_args__ = (
        CheckConstraint("total_seats > 0", name="chk_flight_total_seats"),
        CheckConstraint("available_seats >= 0", name="chk_flight_available_seats_non_negative"),
        CheckConstraint("available_seats <= total_seats", name="chk_flight_available_le_total"),
        CheckConstraint("price > 0", name="chk_flight_price"),
        CheckConstraint("origin_iata <> destination_iata", name="chk_flight_airports"),
        CheckConstraint("arrival_time > departure_time", name="chk_flight_times"),
        CheckConstraint(
            "status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED')",
            name="chk_flight_status",
        ),
        UniqueConstraint("flight_number", "departure_time", name="uq_flight_number_departure"),
    )


class SeatReservation(Base):
    __tablename__ = "seat_reservations"

    id = Column(UUID(as_uuid=True), primary_key=True)
    booking_id = Column(UUID(as_uuid=True), nullable=False, unique=True)
    flight_id = Column(UUID(as_uuid=True), ForeignKey("flights.id"), nullable=False)

    seat_count = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False)

    reserved_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    expires_at = Column(DateTime(timezone=True), nullable=True)
    released_at = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    flight = relationship("Flight", back_populates="reservations")

    __table_args__ = (
        CheckConstraint("seat_count > 0", name="chk_reservation_seat_count"),
        CheckConstraint(
            "status IN ('ACTIVE','RELEASED','EXPIRED')",
            name="chk_reservation_status",
        ),
    )