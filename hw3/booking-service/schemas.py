from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class BookingCreateRequest(BaseModel):
    user_id: UUID
    flight_id: UUID
    passenger_name: str = Field(min_length=1, max_length=255)
    passenger_email: EmailStr
    seat_count: int = Field(gt=0)


class BookingResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    flight_id: UUID
    passenger_name: str
    passenger_email: str
    seat_count: int
    price_snapshot: Decimal
    total_price: Decimal
    status: str
    created_at: datetime
    updated_at: datetime
    cancelled_at: datetime | None = None


class FlightItemResponse(BaseModel):
    id: str
    flight_number: str
    airline: str
    origin_iata: str
    destination_iata: str
    departure_time: datetime
    arrival_time: datetime
    total_seats: int
    available_seats: int
    price: float
    status: str