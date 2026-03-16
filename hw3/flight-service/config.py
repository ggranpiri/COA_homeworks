import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://flight_user:flight_pass@localhost:5432/flight_db",
)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "300"))

GRPC_API_KEY = os.getenv("GRPC_API_KEY")
FLIGHT_SERVICE_PORT = int(os.getenv("FLIGHT_SERVICE_PORT", "50051"))