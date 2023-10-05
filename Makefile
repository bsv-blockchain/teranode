SHELL=/bin/bash

.PHONY: all
all: deps install lint build test

.PHONY: deps
deps:
	go mod download

.PHONY: dev
dev:
	$(MAKE) dev-dashboard & $(MAKE) dev-ubsv

.PHONY: dev-ubsv
dev-ubsv:
	# Run go project
	trap 'kill %1 %2' SIGINT; \
	go run .

.PHONY: dev-dashboard
dev-dashboard:
	# Run node project
	trap 'kill %1 %2' SIGINT; \
	npm install --prefix ./ui/dashboard && npm run dev --prefix ./ui/dashboard

.PHONY: build
build: build-dashboard build-ubsv build-status build-tx-blaster build-dumb-blaster build-aerospiketest

.PHONY: build-ubsv
build-ubsv: build-dashboard
	go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o ubsv.run .

.PHONY: build-tx-blaster
build-tx-blaster:
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o blaster.run ./cmd/txblaster/

.PHONY: build-dumb-blaster
build-dumb-blaster:
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=-N -l" -o dumbblaster.run ./cmd/dumb_blaster/

.PHONY: build-status
build-status:
	go build -o status.run ./cmd/status/

.PHONY: build-aerospiketest
build-aerospiketest:
	go build -o aerospiketest.run ./cmd/aerospiketest/

.PHONY: build-dashboard
build-dashboard:
	npm install --prefix ./ui/dashboard && npm run build --prefix ./ui/dashboard

.PHONY: test
test:
	SETTINGS_CONTEXT=test go test -race -count=1 $$(go list ./... | grep -v playground | grep -v poc)

.PHONY: longtests
longtests:
	SETTINGS_CONTEXT=test LONG_TESTS=1 go test -tags fulltest -race -count=1 -coverprofile=coverage.out $$(go list ./... | grep -v playground | grep -v poc)


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

	# --chainhash_out=. \

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

.PHONY: gen-drpc
gen-drpc:
	# go install storj.io/drpc/cmd/protoc-gen-go-drpc
	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-drpc_out=. \
	--go-drpc_opt=paths=source_relative \
	services/blockassembly/blockassembly_api/blockassembly_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-drpc_out=. \
	--go-drpc_opt=paths=source_relative \
	services/propagation/propagation_api/propagation_api.proto

.PHONY: gen-frpc
gen-frpc:
	# go install github.com/loopholelabs/frpc-go/protoc-gen-go-frpc@2efa3315a5871a40672a95c6a143b789a2249512
	# latest changes have been released, frpc is in alpha stage
	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-frpc_out=. \
	--go-frpc_opt=paths=source_relative \
	services/blockassembly/blockassembly_api/blockassembly_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-frpc_out=. \
	--go-frpc_opt=paths=source_relative \
	services/propagation/propagation_api/propagation_api.proto

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
	rm -f coverage.out

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
	brew install pre-commit
	pre-commit install
