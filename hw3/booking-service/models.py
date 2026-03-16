from sqlalchemy import CheckConstraint, Column, DateTime, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID

from db import Base


class Booking(Base):
    __tablename__ = "bookings"

    id = Column(UUID(as_uuid=True), primary_key=True)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    flight_id = Column(UUID(as_uuid=True), nullable=False)

    passenger_name = Column(String(255), nullable=False)
    passenger_email = Column(String(255), nullable=False)

    seat_count = Column(Integer, nullable=False)

    price_snapshot = Column(Numeric(12, 2), nullable=False)
    total_price = Column(Numeric(12, 2), nullable=False)

    status = Column(String(20), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    cancelled_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint("seat_count > 0", name="chk_booking_seat_count"),
        CheckConstraint("price_snapshot > 0", name="chk_booking_price_snapshot"),
        CheckConstraint("total_price > 0", name="chk_booking_total_price"),
        CheckConstraint(
            "status IN ('CONFIRMED','CANCELLED')",
            name="chk_booking_status",
        ),
    )