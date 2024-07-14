SHELL=/bin/bash

DEBUG_FLAGS=
RACE_FLAGS=
TXMETA_TAG=

.PHONY: set_debug_flags
set_debug_flags:
ifeq ($(DEBUG),true)
	$(eval DEBUG_FLAGS = -N -l)
endif

.PHONY: set_race_flag
set_race_flag:
ifeq ($(RACE),true)
	$(eval RACE_FLAG = -race)
endif

.PHONY: set_txmetacache_flag
set_race_flag:
ifeq ($(TXMETA_SMALL_TAG),true)
	$(eval TXMETA_TAG = "smalltxmetacache")
else
	$(eval TXMETA_TAG = "largetxmetacache")
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
build-ubsv: build-dashboard set_debug_flags set_race_flag set_txmetacache_flag
	go build $(RACE_FLAG) -tags aerospike,native,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o ubsv.run .

.PHONY: build-ubsv-ci
build-ubsv-ci: set_debug_flags set_race_flag set_txmetacache_flag
	go build $(RACE_FLAG) -tags aerospike,native,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o ubsv.run .

.PHONY: build-chainintegrity
build-chainintegrity: set_debug_flags set_race_flag
	go build -tags aerospike,native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o chainintegrity.run ./cmd/chainintegrity/

.PHONY: build-tx-blaster
build-tx-blaster: set_debug_flags set_race_flag
	go build $(RACE_FLAG) -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o blaster.run ./cmd/txblaster/

# .PHONY: build-propagation-blaster
# build-propagation-blaster: set_debug_flags
# 	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o propagationblaster.run ./cmd/propagation_blaster/

# .PHONY: build-utxostore-blaster
# build-utxostore-blaster: set_debug_flags
# 	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o utxostoreblaster.run ./cmd/utxostore_blaster/

# .PHONY: build-s3-blaster
# build-s3-blaster: set_debug_flags
# 	go build -tags native --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o s3blaster.run ./cmd/s3_blaster/

# .PHONY: build-blockassembly-blaster
# build-blockassembly-blaster: set_debug_flags
# 	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o blockassemblyblaster.run ./cmd/blockassembly_blaster/main.go

.PHONY: build-blockchainstatus
build-blockchainstatus:
	go build -o blockchainstatus.run ./cmd/blockchainstatus/

# .PHONY: build-aerospiketest
# build-aerospiketest:
# 	go build -o aerospiketest.run ./cmd/aerospiketest/

.PHONY: build-dashboard
build-dashboard:
	npm install --prefix ./ui/dashboard && npm run build --prefix ./ui/dashboard

.PHONY: install-tools
install-tools:
	go install github.com/ctrf-io/go-ctrf-json-reporter/cmd/go-ctrf-json-reporter@latest

.PHONY: test
test: set_race_flag
ifeq ($(USE_JSON_REPORTER),true)
	$(MAKE) install-tools
	SETTINGS_CONTEXT=test go test -json $(RACE_FLAG) -count=1 $$(go list ./... | grep -v playground | grep -v poc | grep -v test/e2e | grep -v test/settings | grep -v test/state | grep -v test/fork | grep -v test/blockassembly | grep -v test/resilience) | go-ctrf-json-reporter -output ctrf-report.json
else
	SETTINGS_CONTEXT=test go test $(RACE_FLAG) -count=1 $$(go list ./... | grep -v playground | grep -v poc | grep -v test/e2e | grep -v test/settings | grep -v test/state | grep -v test/fork | grep -v test/blockassembly | grep -v test/resilience)
endif

.PHONY: longtests
longtests: set_race_flag
ifeq ($(USE_JSON_REPORTER),true)
	$(MAKE) install-tools
	SETTINGS_CONTEXT=test LONG_TESTS=1 go test -json -tags fulltest $(RACE_FLAG) -count=1 -coverprofile=coverage.out $$(go list ./... | grep -v playground | grep -v poc | grep -v test/e2e | grep -v test/settings | grep -v test/state | grep -v test/fork | grep -v test/blockassembly | grep -v test/resilience) | go-ctrf-json-reporter -output ctrf-report.json
else
	SETTINGS_CONTEXT=test LONG_TESTS=1 go test -tags fulltest $(RACE_FLAG) -count=1 -coverprofile=coverage.out $$(go list ./... | grep -v playground | grep -v poc | grep -v test/e2e | grep -v test/settings | grep -v test/state | grep -v test/fork | grep -v test/blockassembly | grep -v test/resilience)
endif

.PHONY: racetest
racetest: set_race_flag
	SETTINGS_CONTEXT=test LONG_TESTS=1 go test -tags fulltest $(RACE_FLAG) -count=1 -coverprofile=coverage.out github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor

.PHONY: testall
testall:
	# call makefile lint command
	$(MAKE) lint
	$(MAKE) longtests

.PHONY: nightly-tests

nightly-tests:
	docker compose -f docker-compose.ci.build.yml build
	$(MAKE) install-tools
	$(MAKE) clean-data;
	cd test/blockassembly && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../blockassembly-ctrf-report.json
	$(MAKE) clean-data;
	cd test/resilience && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../resilience-ctrf-report.json
	$(MAKE) clean-data;
	cd test/settings && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../settings-ctrf-report.json
	$(MAKE) clean-data;
	cd test/state && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../state-ctrf-report.json
	$(MAKE) clean-data;
	cd test/fork && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../fork-ctrf-report.json

clean-data:
	rm -rf data
	unzip data.zip
	chmod -R +x data
	sleep 2

.PHONY: smoketests

# Default target
smoketests: 
ifdef no-build
	@echo "Skipping build step."
else
	docker compose -f docker-compose.ci.build.yml build
endif
ifdef no-reset
	@echo "Skipping reset step."
else
	rm -rf data
	unzip data.zip
	chmod -R +x data
	sleep 2
endif
ifdef kill-docker
	docker compose -f docker-compose.yml down
endif
ifdef test
	# TEST_DIR := "$(firstword $(subst ., ,$(test)))"
	# TEST_NAME := "$(word 2,$(subst ., ,$(test)))"
	cd test/$(firstword $(subst ., ,$(test))) && \
	SETTINGS_CONTEXT=docker.ci go test -run $(word 2,$(subst ., ,$(test)))
else
	cd test/e2e && \
	SETTINGS_CONTEXT=docker.ci.tc1.run go test
	cd test/blockassembly && \
	SETTINGS_CONTEXT=docker.ci go test
endif


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
	errors/error.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	stores/utxo/status.proto

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
	services/subtreevalidation/subtreevalidation_api/subtreevalidation_api.proto

	# protoc \
	# --proto_path=. \
	# --go_out=. \
	# --go_opt=paths=source_relative \
	# --go-grpc_out=. \
	# --go-grpc_opt=paths=source_relative \
	# services/txmeta/txmeta_api/txmeta_api.proto

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
	rm -f ./services/subtreevalidation/subtreevalidation_api/*.pb.go
	rm -f ./services/validator/validator_api/*.pb.go
	rm -f ./services/propagation/propagation_api/*.pb.go
	# rm -f ./services/txmeta/txmeta_api/*.pb.go
	rm -f ./services/blockchain/blockchain_api/*.pb.go
	rm -f ./services/asset/asset_api/*.pb.go
	rm -f ./services/bootstrap/bootstrap_api/*.pb.go
	rm -f ./services/coinbase/coinbase_api/*.pb.go
	rm -f ./model/*.pb.go
	rm -f ./ubsverrors/*.pb.go
	rm -f ./stores/utxo/*.pb.go

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

.PHONY: generate_fsm_diagram
generate_fsm_diagram:
	go run ./services/blockchain/fsm_visualizer/main.go
	echo "State Machine diagram generated in docs/state-machine.diagram.md"
