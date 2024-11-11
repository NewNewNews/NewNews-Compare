from concurrent import futures
import os
import grpc
from pymongo import MongoClient
from dotenv import load_dotenv

from protos import compare_pb2
from protos import compare_pb2_grpc
from database import ComparisonDatabase

# Load environment variables
load_dotenv()


class ComparisonService(compare_pb2_grpc.ComparisonServiceServicer):
    def __init__(self):
<<<<<<< HEAD
        # Initialize MongoDB client
        self.db = ComparisonDatabase(os.getenv("MONGODB_URI"), os.getenv("MONGODB_DB"), os.getenv("MONGODB_COLLECTION"))

    def GetComparison(self, request, context):
        # Retrieve a comparison from MongoDB based on the `event_id`, and `date`
        event_id = request.event_id
        date = request.date
        
        # print(event_id, date)
        
        result = self.db.get_comparison(event_id, date)
        
        # print(result)
=======
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
>>>>>>> origin/main

        if result:
            # Create a Comparison message to hold the response data
            comparison = compare_pb2.Comparison()

            # Map similarities and differences to KeyValue pairs
            entries = {
<<<<<<< HEAD
                "similarities": result['comparison']['similarities'],
                "differences": result['comparison']['differences'],
=======
                "similarities": result.get("similarities", []),
                "differences": result.get("differences", []),
>>>>>>> origin/main
            }

            for key, values in entries.items():
                kv_entry = comparison.entries.add()
                kv_entry.key = key
                kv_entry.values.extend(values)

            return comparison

        # Handle case where no result is found
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(f"Comparison with event ID {event_id} and date {date} not found")
        return compare_pb2.Comparison()


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    compare_pb2_grpc.add_ComparisonServiceServicer_to_server(
        ComparisonService(), server
    )
<<<<<<< HEAD
    port = os.getenv("PORT", "50053")
    server.add_insecure_port(f"[::]:{port}")
    print(f"Server starting on port {port}...")
=======
    server.add_insecure_port("[::]:50054")
    print("Server starting on port 50054...")
>>>>>>> origin/main
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
