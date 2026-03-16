import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://booking_user:booking_pass@localhost:5432/booking_db",
)

FLIGHT_SERVICE_HOST = os.getenv("FLIGHT_SERVICE_HOST", "localhost")
FLIGHT_SERVICE_PORT = int(os.getenv("FLIGHT_SERVICE_PORT", "50051"))
GRPC_API_KEY = os.getenv("GRPC_API_KEY")

GRPC_TIMEOUT_SECONDS = float(os.getenv("GRPC_TIMEOUT_SECONDS", "2"))
GRPC_RETRY_ATTEMPTS = int(os.getenv("GRPC_RETRY_ATTEMPTS", "3"))
GRPC_BACKOFF_1 = float(os.getenv("GRPC_BACKOFF_1", "0.1"))
GRPC_BACKOFF_2 = float(os.getenv("GRPC_BACKOFF_2", "0.2"))
GRPC_BACKOFF_3 = float(os.getenv("GRPC_BACKOFF_3", "0.4"))

CB_FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
CB_RECOVERY_TIMEOUT = float(os.getenv("CB_RECOVERY_TIMEOUT", "15"))
CB_HALF_OPEN_MAX_CALLS = int(os.getenv("CB_HALF_OPEN_MAX_CALLS", "1"))