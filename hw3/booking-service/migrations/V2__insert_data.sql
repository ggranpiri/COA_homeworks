INSERT INTO bookings (
    id,
    user_id,
    flight_id,
    passenger_name,
    passenger_email,
    seat_count,
    price_snapshot,
    total_price,
    status,
    created_at,
    updated_at
)
VALUES
(
    'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa',
    'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb',
    '11111111-1111-1111-1111-111111111111',
    'Test Passenger',
    'test@example.com',
    1,
    7500.00,
    7500.00,
    'CONFIRMED',
    now(),
    now()
)
ON CONFLICT (id) DO NOTHING;