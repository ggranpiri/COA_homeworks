import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://flight_user:flight_pass@localhost:5432/flight_db",
)

REDIS_MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")
REDIS_SENTINEL_HOST = os.getenv("REDIS_SENTINEL_HOST", "localhost")
REDIS_SENTINEL_PORT = int(os.getenv("REDIS_SENTINEL_PORT", "26379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", "300"))

GRPC_API_KEY = os.getenv("GRPC_API_KEY")
FLIGHT_SERVICE_PORT = int(os.getenv("FLIGHT_SERVICE_PORT", "50051"))