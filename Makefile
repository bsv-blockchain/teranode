SHELL=/bin/bash

DEBUG_FLAGS=
TXMETA_TAG=
SETTINGS_CONTEXT_DEFAULT := docker.ci
LOCAL_TEST_START_FROM_STATE ?=

.PHONY: set_debug_flags
set_debug_flags:
ifeq ($(DEBUG),true)
	$(eval DEBUG_FLAGS = -N -l)
endif

.PHONY: set_txmetacache_flag
set_txmetacache_flag:
ifeq ($(TXMETA_SMALL_TAG),true)
	$(eval TXMETA_TAG = "smalltxmetacache")
else ifeq ($(TXMETA_TEST_TAG),true)
   	$(eval TXMETA_TAG = "testtxmetacache")
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
	$(MAKE) dev-dashboard & $(MAKE) dev-teranode

.PHONY: dev-teranode
dev-teranode:
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
build: update_config build-teranode-with-dashboard build-teranode-cli clean_backup

.PHONY: update_config
update_config:
ifeq ($(LOCAL_TEST_START_FROM_STATE),)
	@echo "No LOCAL_TEST_START_FROM_STATE provided; using existing settings_local.conf"
else
	@echo "Updating settings_local.conf with local_test_start_from_state=$(LOCAL_TEST_START_FROM_STATE)"

	# Remove existing local_test_start_from_state line if it exists
	# For macOS (BSD sed):
	@sed -i '' '/^[[:space:]]*local_test_start_from_state[[:space:]]*=.*$$/d' settings_local.conf

	# For Linux (GNU sed), comment out the above line and uncomment the following line:
	# @sed -i '/^[[:space:]]*local_test_start_from_state[[:space:]]*=.*$$/d' settings_local.conf

	# Append an empty line
	@echo "" >> settings_local.conf
	# Append the new local_test_start_from_state value
	@echo "local_test_start_from_state = $(LOCAL_TEST_START_FROM_STATE)" >> settings_local.conf
endif


.PHONY: clean_backup
clean_backup:
	@rm -f settings_local.conf.bak


.PHONY: build-teranode-with-dashboard
build-teranode-with-dashboard: set_debug_flags set_txmetacache_flag build-dashboard
	go build -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL -X main.StartFromState=${START_FROM_STATE}"  -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-teranode
build-teranode: set_debug_flags set_txmetacache_flag
	go build -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-teranode-no-debug
build-teranode-no-debug: set_txmetacache_flag
	go build -a -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL -s -w" -gcflags "-l -B" -o teranode_no_debug.run .

.PHONY: build-teranode-ci
build-teranode-ci: set_debug_flags set_txmetacache_flag
	go build -race -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-chainintegrity
build-chainintegrity: set_debug_flags
	go build -o chainintegrity.run ./compose/cmd/chainintegrity/

.PHONY: build-tx-blaster
build-tx-blaster: set_debug_flags
	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o blaster.run ./cmd/txblaster/

.PHONY: build-teranode-cli
build-teranode-cli:
	go build -o teranode-cli ./cmd/teranodecli

# .PHONY: build-propagation-blaster
# build-propagation-blaster: set_debug_flags
# 	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o propagationblaster.run ./cmd/propagation_blaster/

# .PHONY: build-utxostore-blaster
# build-utxostore-blaster: set_debug_flags
# 	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o utxostoreblaster.run ./cmd/utxostore_blaster/

# .PHONY: build-s3-blaster
# build-s3-blaster: set_debug_flags
# 	go build --trimpath -ldflags="-X main.commit=${GITHUB_SHA} -X main.version=MANUAL" -gcflags "all=${DEBUG_FLAGS}" -o s3blaster.run ./cmd/s3_blaster/

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

# make test will run all tests in the project except for the ones in the test directory
.PHONY: test
test:
	SETTINGS_CONTEXT=test go list ./... | grep -v github.com/bitcoin-sv/teranode/test | xargs go test -race -tags "testtxmetacache" -count=1 -coverprofile=coverage.out 2>&1 | grep -v "ld: warning:"

# run tests in the test/longtest directory
.PHONY: longtest
longtest:
	SETTINGS_CONTEXT=test go test -race -tags "testtxmetacache" -count=1 -timeout 180 -coverprofile=coverage.out ./test/longtest/... 2>&1 | grep -v "ld: warning:"

# run tests in the test/sequentialtest directory in order, one by one
.PHONY: sequentialtest
sequentialtest:
	logLevel=INFO test/scripts/run_tests_sequentially.sh

.PHONY: testall
testall: test longtest sequentialtest

# run tests in the test/e2e/daemon directory
.PHONY: smoketest
smoketest:
	SETTINGS_CONTEXT=test go test -race -tags "testtxmetacache" -count=1 -timeout 180 -coverprofile=coverage.out ./test/e2e/daemon/... 2>&1 | grep -v "ld: warning:"

.PHONY: nightly-tests
nightly-tests:
	docker compose -f docker-compose.ci.build.yml build
	$(MAKE) install-tools

	cd $(test_dir) && SETTINGS_CONTEXT=$(or $(settings_context),$(SETTINGS_CONTEXT_DEFAULT)) go test -v -tags $(test_tags) -json | go-ctrf-json-reporter -output ../../$(report_name) --verbose
	# cd $(TEST_DIR) && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../$(REPORT_NAME) --verbose

reset-data:
	unzip data.zip
	chmod -R u+w data

.PHONY: smoketests
smoketests:
	cd test/smoke && \
		go test -v -count 1 -tags test_smoke_rpc ./...

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
	services/alert/alert_api/alert_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/legacy/peer_api/peer_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	services/p2p/p2p_api/p2p_api.proto

	protoc \
	--proto_path=. \
	--go_out=. \
	--go_opt=paths=source_relative \
	--go-grpc_out=. \
	--go-grpc_opt=paths=source_relative \
	util/kafka/kafka_message/kafka_messages.proto


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
	rm -f ./services/coinbase/coinbase_api/*.pb.go
	rm -f ./services/legacy/peer_api/*.pb.go
	rm -f ./services/p2p/p2p_api/*.pb.go
	rm -f ./model/*.pb.go
	rm -f ./errors/*.pb.go
	rm -f ./stores/utxo/*.pb.go

.PHONY: clean
clean:
	rm -f ./teranode_*.tar.gz
	rm -f blaster.run
	rm -f blockchainstatus.run
	rm -rf build/
	rm -f coverage.out

.PHONY: install-lint
install-lint:
	brew install golangci-lint
	brew install staticcheck


# lint will check the changed files in the current branch compared to main, including commits and unstaged/untracked changes
# It will show new linting errors/warnings, by updating local copy of origin/main with the latest state of the remote main branch.
.PHONY: lint
lint:
	git fetch origin main
	golangci-lint run ./... --new-from-rev origin/main

# lint-new will only check only your unstaged/untracked changes (not committed changes), or fallback to check last commit if no changes in checkout
# It is useful for quickly checking that your current, uncommitted work doesn’t introduce new lint errors.
.PHONY: lint-new
lint-new:
	golangci-lint run ./... --new

# lint-full will check all files in the project
# It will show all lint errors and warnings.
.PHONY: lint-full
lint-full:
	golangci-lint run ./...

# lint-full-changed-dirs will check the files that have been added/modified in the current branch compared to base main, including unstaged/untracked changes
# It will show all lint errors and warnings.
.PHONY: lint-full-changed-dirs
lint-full-changed-dirs:
	@base_commit=$$(git merge-base main HEAD); \
	echo "Using base commit $$base_commit for diffing"; \
	changed_dirs=$$(git diff --name-only $$base_commit HEAD | grep '\.go$$' | xargs -I{} dirname {} | sort -u); \
	if [ -z "$$changed_dirs" ]; then \
	  echo "No changed Go files found."; \
	else \
	  echo "Linting packages in the following directories:"; \
	  echo "$$changed_dirs"; \
	  golangci-lint run $$changed_dirs; \
	fi

# The install target installs all dependencies needed for development.
# Dependencies are categorized as:
# - Core: Required for all development tasks (protobuf tools)
# - Build: Required for specific build operations (libtool, autoconf, automake)
# - Quality: Tools for code quality (linting)
# - Workflow: Required for team collaboration (git hooks)
.PHONY: install
install:
	# Quality tools (optional but recommended)
	$(MAKE) install-lint
	# Core dependencies (required for gRPC service development)
	brew install protobuf
	brew install protoc-gen-go
	brew install protoc-gen-go-grpc
	# Build dependencies (required for certain native code components)
	brew install libtool
	brew install autoconf
	brew install automake
	# Workflow tools (required for team collaboration)
	brew install pre-commit
	pre-commit install

.PHONY: generate_fsm_diagram
generate_fsm_diagram:
	go run ./services/blockchain/fsm_visualizer/main.go
	echo "State Machine diagram generated in docs/state-machine.diagram.md"
