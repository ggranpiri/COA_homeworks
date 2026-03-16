from config import GRPC_API_KEY


def grpc_metadata():
    return (("x-api-key", GRPC_API_KEY),)