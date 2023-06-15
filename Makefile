SHELL=/bin/bash

.PHONY: all
all: deps lint build test

.PHONY: deps
deps:
	go mod download

.PHONY: build
build:
	sh build.sh

.PHONY: test
test:
	go test -race -count=1 ./...

.PHONY: lint
lint:
	golangci-lint run --skip-dirs p2p/wire
	staticcheck ./...

.PHONY: run
run:
	sh run.sh

.PHONY: gen
gen:
	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	model/model.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/validator/validator_api/validator_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/utxo/utxostore_api/utxostore_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/propagation/propagation_api/propagation_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/blockassembly/blockassembly_api/blockassembly_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/blockvalidation/blockvalidation_api/blockvalidation_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/seeder/seeder_api/seeder_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/txstatus/txstatus_api/txstatus_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/blockchain/blockchain_api/blockchain_api.proto

.PHONY: clean_gen
clean_gen:
	rm -f ./services/blockassembly/blockassembly_api/*.pb.go
	rm -f ./services/blockvalidation/blockvalidation_api/*.pb.go
	rm -f ./services/seeder/seeder_api/*.pb.go
	rm -f ./services/validator/validator_api/*.pb.go
	rm -f ./services/utxo/utxostore_api/*.pb.go
	rm -f ./services/propagation/propagation_api/*.pb.go
	rm -f ./services/txstatus/txstatus_api/*.pb.go
	rm -f ./services/blockchain/blockchain_api/*.pb.go
	rm -f ./model/*.pb.go

.PHONY: clean
clean:
	rm -f ./ubsv_*.tar.gz
	rm -rf build/

.PHONY: install
install:
	# arch -arm64 brew install golangci-lint
	arch -arm64 brew install pre-commit
	pre-commit install
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
