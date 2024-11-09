PROTO_DIR := protos
GOOGLE_APIS_DIR := $(PROTO_DIR)/google/api

.PHONY: all clean proto

all: proto

clean:
	rm -rf $(GOOGLE_APIS_DIR)
	rm -f $(PROTO_DIR)/*.pb.go

$(GOOGLE_APIS_DIR):
	mkdir -p $(GOOGLE_APIS_DIR)
	curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o $(GOOGLE_APIS_DIR)/annotations.proto
	curl -sSL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o $(GOOGLE_APIS_DIR)/http.proto

proto: $(GOOGLE_APIS_DIR)

	python -m grpc_tools.protoc -I . \
		-I$(PROTO_DIR) \
		--python_out=. \
		--grpc_python_out=. \
		$(PROTO_DIR)/compare.proto