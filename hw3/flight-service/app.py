from concurrent import futures

import grpc

from config import FLIGHT_SERVICE_PORT
from grpc_server import FlightServiceServicer
from proto import flight_pb2_grpc


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    flight_pb2_grpc.add_FlightServiceServicer_to_server(FlightServiceServicer(), server)
    server.add_insecure_port(f"[::]:{FLIGHT_SERVICE_PORT}")
    server.start()
    print(f"Flight Service started on port {FLIGHT_SERVICE_PORT}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()