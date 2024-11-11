build:
	docker compose up -d --build

start:
	docker compose up -d

stop:
	sudo docker compose down

protos:
	python -m grpc_tools.protoc -I./protos --python_out=./protos/ --grpc_python_out=./protos/ ./protos/compare.proto

client:
	python client.py