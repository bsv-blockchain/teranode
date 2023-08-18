SHELL=/bin/bash

.PHONY: all
all: deps install lint build test

.PHONY: deps
deps:
	go mod download

.PHONY: build
build: build-ubsv build-status build-tx-blaster

.PHONY: build-ubsv
build-ubsv:
	go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run .

.PHONY: build-tx-blaster
build-tx-blaster:
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o blaster.run ./cmd/txblaster/

.PHONY: build-status
build-status:
	go build -o status.run ./cmd/status/

.PHONY: test
test:
	go test -race -count=1 $$(go list ./... | grep -v playground | grep -v poc)

.PHONY: longtests
longtests:
	LONG_TESTS=1 go test -race -count=1 $$(go list ./... | grep -v playground | grep -v poc)


.PHONY: testall
testall:
	# call makefile lint command
	$(MAKE) lint
	$(MAKE) longtests


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
	services/txmeta/txmeta_api/txmeta_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/blockchain/blockchain_api/blockchain_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/blobserver/blobserver_api/blobserver_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/bootstrap/bootstrap_api/bootstrap_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/coinbase/coinbase_api/coinbase_api.proto

.PHONY: clean_gen
clean_gen:
	rm -f ./services/blockassembly/blockassembly_api/*.pb.go
	rm -f ./services/blockvalidation/blockvalidation_api/*.pb.go
	rm -f ./services/seeder/seeder_api/*.pb.go
	rm -f ./services/validator/validator_api/*.pb.go
	rm -f ./services/utxo/utxostore_api/*.pb.go
	rm -f ./services/propagation/propagation_api/*.pb.go
	rm -f ./services/txmeta/txmeta_api/*.pb.go
	rm -f ./services/blockchain/blockchain_api/*.pb.go
	rm -f ./services/blobserver/blobserver_api/*.pb.go
	rm -f ./services/bootstrap/bootstrap_api/*.pb.go
	rm -f ./services/coinbase/coinbase_api/*.pb.go
	rm -f ./model/*.pb.go

.PHONY: clean
clean:
	rm -f ./ubsv_*.tar.gz
	rm -f blaster.run
	rm -f status.run
	rm -rf build/

.PHONY: install-lint
install-lint:
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: lint
lint: # todo enable coinbase tracker
	golangci-lint run ./...
	staticcheck ./...

.PHONY: install
install:
	$(MAKE) install-lint
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	pip install pre-commit
	pre-commit install
