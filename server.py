# gRPC, get data from mongo by new_id
from concurrent import futures
import os

import grpc
from pymongo import MongoClient
from dotenv import load_dotenv

from protos import compare_pb2
from protos import compare_pb2_grpc


class ComparisonService(compare_pb2_grpc.ComparisonServiceServicer):
    def __init__(self):
        # Initialize MongoDB client
        self.mongo_client = MongoClient(os.getenv("MONGO_URI"))
        self.db = self.mongo_client[os.getenv("MONGO_DB")]
        self.collection = self.db[os.getenv("MONGO_COLLECTION")]

    def GetComparison(self, request, context):
        # Retrieve a comparison from MongoDB based on the news_id
        try:
            news_id = int(request.news_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("news_id must be an integer")
            return compare_pb2.Comparison()

        result = self.collection.find_one({"news_id": news_id})

        if result:

            comparison = compare_pb2.Comparison()

            entries = {
                "similarities": result["similarities"],
                "differences": result["differences"],
            }

            for k, v in entries.items():
                kv = comparison.entries.add()
                kv.key = k
                kv.values.extend(v)

            return comparison

        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Comparison with ID {news_id} not found")
        return compare_pb2.Comparison()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compare_pb2_grpc.add_ComparisonServiceServicer_to_server(
        ComparisonService(), server
    )
    server.add_insecure_port("[::]:50051")
    print("Server starting on port 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    load_dotenv()
    serve()
