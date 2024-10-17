import grpc
from protos import compare_pb2
from protos import compare_pb2_grpc


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = compare_pb2_grpc.ComparisonServiceStub(channel)

        while True:
            # Send news ID to get comparison
            news_id = input("Enter the news ID: ")
            if not news_id:
                break
            try:
                response = stub.GetComparison(
                    compare_pb2.GetComparisonRequest(news_id=news_id)
                )
            except grpc.RpcError as e:
                print(f"An error occurred: {e}")
                continue

            if response.entries:
                print(f"Received comparison data for news ID: {news_id}")
                for entry in response.entries:
                    print(f"Key: {entry.key}")
                    for value in entry.values:
                        print(f"  Value: {value}")
            else:
                print("No comparison data found")


if __name__ == "__main__":
    run()
