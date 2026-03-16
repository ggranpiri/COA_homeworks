CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE bookings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    user_id UUID NOT NULL,
    flight_id UUID NOT NULL,

    passenger_name VARCHAR(255) NOT NULL,
    passenger_email VARCHAR(255) NOT NULL,

    seat_count INT NOT NULL CHECK (seat_count > 0),

    price_snapshot NUMERIC(12,2) NOT NULL CHECK (price_snapshot > 0),
    total_price NUMERIC(12,2) NOT NULL CHECK (total_price > 0),

    status VARCHAR(20) NOT NULL CHECK (
        status IN ('CONFIRMED','CANCELLED')
    ),

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at TIMESTAMPTZ
);

CREATE INDEX idx_bookings_user
ON bookings(user_id);

CREATE INDEX idx_bookings_flight
ON bookings(flight_id);

CREATE INDEX idx_bookings_status
ON bookings(status);