SHELL=/bin/bash

DEBUG_FLAGS=

.PHONY: set_debug_flags
set_debug_flags:
ifeq ($(DEBUG),true)
	$(eval DEBUG_FLAGS = -N -l)
endif

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
# build-blockchainstatus build-tx-blaster build-propagation-blaster build-aerospiketest build-blockassembly-blaster build-utxostore-blaster build-s3-blaster build-chainintegrity
build: build-dashboard build-ubsv

.PHONY: build-ubsv
build-ubsv: set_debug_flags
	go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o ubsv.run .

.PHONY: build-chainintegrity
build-chainintegrity: set_debug_flags
	go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o chainintegrity.run ./cmd/chainintegrity/

.PHONY: build-tx-blaster
build-tx-blaster: set_debug_flags
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o blaster.run ./cmd/txblaster/

.PHONY: build-propagation-blaster
build-propagation-blaster: set_debug_flags
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o propagationblaster.run ./cmd/propagation_blaster/

.PHONY: build-utxostore-blaster
build-utxostore-blaster: set_debug_flags
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o utxostoreblaster.run ./cmd/utxostore_blaster/

.PHONY: build-s3-blaster
build-s3-blaster: set_debug_flags
	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o s3blaster.run ./cmd/s3_blaster/

.PHONY: build-blockassembly-blaster
build-blockassembly-blaster: set_debug_flags
	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o blockassemblyblaster.run ./cmd/blockassembly_blaster/main.go

.PHONY: build-blockchainstatus
build-blockchainstatus:
	go build -o blockchainstatus.run ./cmd/blockchainstatus/

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

.PHONY: racetest
racetest:
	SETTINGS_CONTEXT=test LONG_TESTS=1 go test -tags fulltest -race -count=1 -coverprofile=coverage.out github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor

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
	ubsverrors/error.proto

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
	services/asset/asset_api/asset_api.proto

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
	services/blockvalidation/blockvalidation_api/blockvalidation_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-frpc_out=. \
	--go-frpc_opt=paths=source_relative \
	services/validator/validator_api/validator_api.proto

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
	rm -f ./services/asset/asset_api/*.pb.go
	rm -f ./services/bootstrap/bootstrap_api/*.pb.go
	rm -f ./services/coinbase/coinbase_api/*.pb.go
	rm -f ./cmd/blockassembly_blaster
	rm -f ./model/*.pb.go
	rm -f ./ubsverrors/*.pb.go

.PHONY: clean
clean:
	rm -f ./ubsv_*.tar.gz
	rm -f blaster.run
	rm -f blockchainstatus.run
	rm -rf build/
	rm -f coverage.out

.PHONY: install-lint
install-lint:
	brew install golangci-lint
	brew install staticcheck

.PHONY: lint
lint: # todo enable coinbase tracker
	golangci-lint run ./...
	staticcheck ./...

.PHONY: install
install:
	$(MAKE) install-lint
	brew install protoc-gen-go
	brew install protoc-gen-go-grpc
	brew install pre-commit
	pre-commit install
