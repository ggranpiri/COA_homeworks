INSERT INTO flights (
    id,
    flight_number,
    airline,
    origin_iata,
    destination_iata,
    departure_time,
    arrival_time,
    total_seats,
    available_seats,
    price,
    status
)
VALUES
(
    '11111111-1111-1111-1111-111111111111',
    'SU100',
    'Aeroflot',
    'SVO',
    'LED',
    '2026-03-20 10:00:00+00',
    '2026-03-20 11:30:00+00',
    100,
    100,
    7500.00,
    'SCHEDULED'
),
(
    '22222222-2222-2222-2222-222222222222',
    'B2101',
    'Belavia',
    'MSQ',
    'LED',
    '2026-03-20 08:00:00+00',
    '2026-03-20 09:30:00+00',
    80,
    80,
    6200.00,
    'SCHEDULED'
),
(
    '33333333-3333-3333-3333-333333333333',
    'SU200',
    'Aeroflot',
    'SVO',
    'KZN',
    '2026-03-21 09:00:00+00',
    '2026-03-21 11:00:00+00',
    50,
    3,
    6800.00,
    'SCHEDULED'
),
(
    '44444444-4444-4444-4444-444444444444',
    'SU300',
    'Aeroflot',
    'SVO',
    'LED',
    '2026-03-22 07:00:00+00',
    '2026-03-22 08:20:00+00',
    120,
    120,
    8200.00,
    'CANCELLED'
)
ON CONFLICT (id) DO NOTHING;