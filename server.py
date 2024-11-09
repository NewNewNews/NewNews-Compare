from concurrent import futures
import os
import grpc
from pymongo import MongoClient
from dotenv import load_dotenv

from protos import compare_pb2
from protos import compare_pb2_grpc

# Load environment variables
load_dotenv()


class ComparisonService(compare_pb2_grpc.ComparisonServiceServicer):
    def __init__(self):
        # Initialize MongoDB client with database and collection
        self.mongo_client = MongoClient(os.getenv("MONGO_URI"))
        # self.db = self.mongo_client[os.getenv("MONGO_DB")]
        # self.collection = self.db[os.getenv("MONGO_COLLECTION")]

    def GetComparison(self, request, context):
        # Attempt to parse news_id as an integer
        try:
            news_id = int(request.news_id)
        except ValueError:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("news_id must be an integer")
            return compare_pb2.Comparison()

        # Query MongoDB for the specified news_id
        result = self.collection.find_one({"news_id": news_id})

        if result:
            # Create a Comparison message to hold the response data
            comparison = compare_pb2.Comparison()

            # Map similarities and differences to KeyValue pairs
            entries = {
                "similarities": result.get("similarities", []),
                "differences": result.get("differences", []),
            }

            for key, values in entries.items():
                kv_entry = comparison.entries.add()
                kv_entry.key = key
                kv_entry.values.extend(values)

            return comparison

        # Handle case where no result is found
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Comparison with ID {news_id} not found")
        return compare_pb2.Comparison()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compare_pb2_grpc.add_ComparisonServiceServicer_to_server(
        ComparisonService(), server
    )
    server.add_insecure_port("[::]:50054")
    print("Server starting on port 50054...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
