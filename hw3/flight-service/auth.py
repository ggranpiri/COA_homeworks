import grpc

from config import GRPC_API_KEY


def check_api_key(context: grpc.ServicerContext) -> bool:
    metadata = dict(context.invocation_metadata())
    return metadata.get("x-api-key") == GRPC_API_KEY