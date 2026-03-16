CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE flights (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    flight_number VARCHAR(20) NOT NULL,
    airline VARCHAR(100) NOT NULL,

    origin_iata CHAR(3) NOT NULL,
    destination_iata CHAR(3) NOT NULL,

    departure_time TIMESTAMPTZ NOT NULL,
    arrival_time TIMESTAMPTZ NOT NULL,

    total_seats INT NOT NULL CHECK (total_seats > 0),
    available_seats INT NOT NULL CHECK (available_seats >= 0),

    price NUMERIC(12,2) NOT NULL CHECK (price > 0),

    status VARCHAR(20) NOT NULL CHECK (
        status IN ('SCHEDULED','DEPARTED','CANCELLED','COMPLETED')
    ),

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_airports CHECK (origin_iata <> destination_iata),
    CONSTRAINT chk_times CHECK (arrival_time > departure_time),
    CONSTRAINT chk_available_seats CHECK (available_seats <= total_seats),

    CONSTRAINT uq_flight UNIQUE (flight_number, departure_time)
);

CREATE INDEX idx_flights_search
ON flights (origin_iata, destination_iata, departure_time);



CREATE TABLE seat_reservations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    booking_id UUID NOT NULL UNIQUE,
    flight_id UUID NOT NULL REFERENCES flights(id),

    seat_count INT NOT NULL CHECK (seat_count > 0),

    status VARCHAR(20) NOT NULL CHECK (
        status IN ('ACTIVE','RELEASED','EXPIRED')
    ),

    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_flight
ON seat_reservations(flight_id);

CREATE INDEX idx_reservations_status
ON seat_reservations(status);