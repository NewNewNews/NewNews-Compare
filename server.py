# gRPC, get data from mongo by new_id
from concurrent import futures
import os

import grpc
from pymongo import MongoClient
from dotenv import load_dotenv

from protos import compare_pb2
from protos import compare_pb2_grpc
from database import ComparisonDatabase


class ComparisonService(compare_pb2_grpc.ComparisonServiceServicer):
    def __init__(self):
        # Initialize MongoDB client
        self.db = ComparisonDatabase(os.getenv("MONGODB_URI"), os.getenv("MONGODB_DB"), os.getenv("MONGODB_COLLECTION"))

    def GetComparison(self, request, context):
        # Retrieve a comparison from MongoDB based on the `event_id`, and `date`
        event_id = request.event_id
        date = request.date
        
        # print(event_id, date)
        
        result = self.db.get_comparison(event_id, date)
        
        # print(result)

        if result:

            comparison = compare_pb2.Comparison()

            entries = {
                "similarities": result['comparison']['similarities'],
                "differences": result['comparison']['differences'],
            }

            for k, v in entries.items():
                kv = comparison.entries.add()
                kv.key = k
                kv.values.extend(v)

            return comparison

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Comparison with event ID {event_id} and date {date} not found")
        return compare_pb2.Comparison()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compare_pb2_grpc.add_ComparisonServiceServicer_to_server(
        ComparisonService(), server
    )
    port = os.getenv("PORT", "50053")
    server.add_insecure_port(f"[::]:{port}")
    print(f"Server starting on port {port}...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    load_dotenv()
    serve()
