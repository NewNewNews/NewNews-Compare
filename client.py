import grpc
from protos import compare_pb2
from protos import compare_pb2_grpc


def run():
    with grpc.insecure_channel("localhost:50054") as channel:
        stub = compare_pb2_grpc.ComparisonServiceStub(channel)

        while True:
            # Send `event_id` and `date` to retrieve a comparison
            event_id = input("Enter event ID: ")
            date = input("Enter date: ")
            if not event_id or not date:
                event_id = "0001"
                date = "2024-11-10"
            try:
                response = stub.GetComparison(
                    compare_pb2.GetComparisonRequest(event_id=event_id, date=date)
                )
            except grpc.RpcError as e:
                print(f"An error occurred: {e}")
                continue

            if response.entries:
                print(f"Received comparison data for event ID: {event_id}")
                for entry in response.entries:
                    print(f"Key: {entry.key}")
                    for value in entry.values:
                        print(f"  Value: {value}")
            else:
                print("No comparison data found")


if __name__ == "__main__":
    run()
