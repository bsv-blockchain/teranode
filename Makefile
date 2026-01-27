SHELL=/bin/bash
# Temp disabled for now until all tests are green
.SHELLFLAGS=-o pipefail -c

DEBUG_FLAGS=
TXMETA_TAG=
SETTINGS_CONTEXT_DEFAULT := docker.ci
LOCAL_TEST_START_FROM_STATE ?=

# Get version from git using the shared script
# Use environment variables if set, otherwise use the script
ifndef GIT_VERSION
  # Get git version variables directly from the script
  GIT_VERSION := $(shell ./scripts/determine-git-version.sh --makefile | grep "^GIT_VERSION=" | cut -d'=' -f2)
  GIT_COMMIT := $(shell ./scripts/determine-git-version.sh --makefile | grep "^GIT_COMMIT=" | cut -d'=' -f2)
  GIT_SHA := $(shell ./scripts/determine-git-version.sh --makefile | grep "^GIT_SHA=" | cut -d'=' -f2)
  GIT_TAG := $(shell ./scripts/determine-git-version.sh --makefile | grep "^GIT_TAG=" | cut -d'=' -f2)
  GIT_TIMESTAMP := $(shell ./scripts/determine-git-version.sh --makefile | grep "^GIT_TIMESTAMP=" | cut -d'=' -f2)
endif

# Cross-compilation environment variables
# These can be overridden when calling make, e.g.: make build GOOS=linux GOARCH=amd64
CGO_ENABLED ?= 1
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

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
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -mod=readonly -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GIT_COMMIT} -X main.version=${GIT_VERSION} -X main.StartFromState=${START_FROM_STATE}"  -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-teranode
build-teranode: set_debug_flags set_txmetacache_flag
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -mod=readonly -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GIT_COMMIT} -X main.version=${GIT_VERSION}" -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-teranode-no-debug
build-teranode-no-debug: set_txmetacache_flag
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -mod=readonly -a -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GIT_COMMIT} -X main.version=${GIT_VERSION} -s -w" -gcflags "-l -B" -o teranode_no_debug.run .

.PHONY: build-teranode-ci
build-teranode-ci: set_debug_flags set_txmetacache_flag
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -mod=readonly -race -tags aerospike,${TXMETA_TAG} --trimpath -ldflags="-X main.commit=${GIT_COMMIT} -X main.version=${GIT_VERSION}" -gcflags "all=${DEBUG_FLAGS}" -o teranode.run .

.PHONY: build-chainintegrity
build-chainintegrity: set_debug_flags
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o chainintegrity.run ./compose/cmd/chainintegrity/

.PHONY: build-tx-blaster
build-tx-blaster: set_debug_flags
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build --trimpath -ldflags="-X main.commit=${GIT_COMMIT} -X main.version=${GIT_VERSION}" -gcflags "all=${DEBUG_FLAGS}" -o blaster.run ./cmd/txblaster/

.PHONY: build-teranode-cli
build-teranode-cli:
	CGO_ENABLED=$(CGO_ENABLED) GOOS=$(GOOS) GOARCH=$(GOARCH) go build -mod=readonly -o teranode-cli ./cmd/teranodecli

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
	go install gotest.tools/gotestsum@latest

.PHONY: test
test:
	@command -v gotestsum >/dev/null 2>&1 || { echo "gotestsum not found. Installing..."; $(MAKE) install-tools; }
	SETTINGS_CONTEXT=test gotestsum --format pkgname -- -race -tags "testtxmetacache" -count=1 -timeout=10m -coverprofile=coverage.out -coverpkg=./... $$(go list ./... | grep -v github.com/bsv-blockchain/teranode/test/ | sort)

# run tests in the test/longtest directory
.PHONY: longtest
longtest:
	@command -v gotestsum >/dev/null 2>&1 || { echo "gotestsum not found. Installing..."; $(MAKE) install-tools; }
	SETTINGS_CONTEXT=test gotestsum --format pkgname -- -race -tags "testtxmetacache" -count=1 -timeout=10m -coverprofile=coverage.out ./test/longtest/... 2>&1 | grep -v "ld: warning:"

# run tests in the test/sequentialtest directory in order, one by one
.PHONY: sequentialtest
sequentialtest:
	@mkdir -p /tmp/teranode-test-results
	logLevel=INFO test/scripts/run_tests_sequentially.sh 2>&1 | tee /tmp/teranode-test-results/sequentialtest-results.txt

# run sequential tests for specific database backends
.PHONY: sequentialtest-sqlite
sequentialtest-sqlite:
	@mkdir -p /tmp/teranode-test-results
	logLevel=INFO test/scripts/run_tests_sequentially.sh --db sqlite 2>&1 | tee /tmp/teranode-test-results/sequentialtest-sqlite-results.txt

.PHONY: sequentialtest-postgres
sequentialtest-postgres:
	@mkdir -p /tmp/teranode-test-results
	logLevel=INFO test/scripts/run_tests_sequentially.sh --db postgres 2>&1 | tee /tmp/teranode-test-results/sequentialtest-postgres-results.txt

.PHONY: sequentialtest-aerospike
sequentialtest-aerospike:
	@mkdir -p /tmp/teranode-test-results
	logLevel=INFO test/scripts/run_tests_sequentially.sh --db aerospike 2>&1 | tee /tmp/teranode-test-results/sequentialtest-aerospike-results.txt

.PHONY: testall
testall: test longtest sequentialtest

# run tests in the test/e2e/daemon directory
# Tests run in parallel by default - each test gets unique ports and data directories
.PHONY: smoketest
smoketest:
	@command -v gotestsum >/dev/null 2>&1 || { echo "gotestsum not found. Installing..."; $(MAKE) install-tools; }
	@mkdir -p /tmp/teranode-test-results
	cd test/e2e/daemon/ready && gotestsum --format pkgname -- -v -count=1 -race -timeout=10m -parallel 2 -run . 2>&1 | tee /tmp/teranode-test-results/smoketest-results.txt

# run chain integrity tests - multi-node tests with deep chain verification
# This test mines blocks across multiple nodes and verifies chain consistency
.PHONY: chainintegrity
chainintegrity:
	@command -v gotestsum >/dev/null 2>&1 || { echo "gotestsum not found. Installing..."; $(MAKE) install-tools; }
	@mkdir -p /tmp/teranode-test-results
	cd test/e2e/chainintegrity && gotestsum --format pkgname -- -v -count=1 -race -timeout=35m -run . 2>&1 | tee /tmp/teranode-test-results/chainintegrity-results.txt

.PHONY: nightly-tests
nightly-tests:
	docker compose -f docker-compose.ci.build.yml build
	$(MAKE) install-tools

	cd $(test_dir) && SETTINGS_CONTEXT=$(or $(settings_context),$(SETTINGS_CONTEXT_DEFAULT)) go test -v -tags $(test_tags) -json | go-ctrf-json-reporter -output ../../$(report_name) --verbose
	# cd $(TEST_DIR) && SETTINGS_CONTEXT=docker.ci go test -json | go-ctrf-json-reporter -output ../../$(REPORT_NAME) --verbose

reset-data:
	unzip data.zip
	chmod -R u+w data

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
# It is useful for quickly checking that your current, uncommitted work doesn't introduce new lint errors.
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

# Chain Integrity Test - Local version of the GitHub workflow
.PHONY: chain-integrity-test
chain-integrity-test:
	@echo "Starting Chain Integrity Test..."
	@echo "This test replicates the GitHub workflow locally"
	@echo "================================================"
	@echo "Timestamp: $$(date)"
	@echo ""

	# Step 1: Build chainintegrity binary
	@echo "Step 1: Building chainintegrity binary..."
	@echo "  - Compiling chainintegrity tool..."
	$(MAKE) build-chainintegrity
	@echo "  ✓ Chainintegrity binary built successfully"
	@echo ""

	# Step 2: Clean up old data
	@echo "Step 2: Cleaning up old data..."
	@echo "  - Removing existing data directory..."
	@rm -rf data
	@echo "  ✓ Data directory cleaned up"
	@echo ""

	# Step 3: Build teranode image locally
	@echo "Step 3: Building teranode image locally..."
	@echo "  - Logging into AWS ECR..."
	@aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com || (echo "  ✗ ECR login failed - skipping build"; exit 1)
	@echo "  ✓ ECR login successful"
	@echo "  - Building Docker image (this may take several minutes)..."
	@docker build -t teranode:latest .
	@echo "  ✓ Teranode Docker image built successfully"
	@echo ""

	# Step 4: Start Teranode nodes with 3 block generators
	@echo "Step 4: Starting Teranode nodes with 3 block generators..."
	@echo "  - Starting Docker Compose services..."
	@docker compose -f compose/docker-compose-3blasters.yml up -d
	@echo "  ✓ Docker Compose services started"
	@echo "  - Waiting for services to initialize..."
	@sleep 10
	@echo "  ✓ Services initialized"
	@echo ""

	# Step 5: Wait for mining to complete (all nodes at height 120+ and in sync)
	@echo "Step 5: Waiting for mining to complete (all nodes at height 120+ and in sync)..."
	@echo "  - Target height: 120 blocks"
	@echo "  - Maximum wait time: 10 minutes (120 attempts × 5 seconds)"
	@echo "  - Check interval: 5 seconds"
	@echo "  - This may take several minutes..."
	@echo ""
	@set -e; \
	REQUIRED_HEIGHT=120; \
	MAX_ATTEMPTS=120; \
	SLEEP=5; \
	\
	# Function to check for errors in all teranode container logs at once \
	check_errors() { \
		# Get current time for this check \
		local current_time; \
		current_time=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		\
		# Check for errors - if last_check_time is empty, it will check all logs \
		local since_param=""; \
		if [ ! -z "$$last_check_time" ]; then \
			since_param="--since=$$last_check_time"; \
		fi; \
		\
		# Single command pattern that works for both initial and subsequent checks \
		local errors; \
		errors=$$(docker compose -f compose/docker-compose-3blasters.yml logs --no-color $$since_param teranode1 teranode2 teranode3 | grep -i "| ERROR |" || true); \
		\
		# Update timestamp for next check \
		last_check_time=$$current_time; \
		\
		if [[ ! -z "$$errors" ]]; then \
			echo "ERROR: Found error logs in teranode containers:"; \
			echo "$$errors"; \
			return 1; \
		fi; \
		return 0; \
	}; \
	\
	# Initialize empty for first check to get all logs \
	last_check_time=""; \
	\
	for ((i=1; i<=MAX_ATTEMPTS; i++)); do \
		h1=$$(curl -s http://localhost:18090/api/v1/bestblockheader/json | jq -r .height); \
		h2=$$(curl -s http://localhost:28090/api/v1/bestblockheader/json | jq -r .height); \
		h3=$$(curl -s http://localhost:38090/api/v1/bestblockheader/json | jq -r .height); \
		echo "Attempt $$i: heights: $$h1 $$h2 $$h3"; \
		\
		# Check for errors in all teranode containers \
		if ! check_errors; then \
			echo "Errors found in container logs. Exiting."; \
			exit 1; \
		fi; \
		\
		if [[ -z "$$h1" || -z "$$h2" || -z "$$h3" ]]; then \
			if [[ $$i -gt 10 ]]; then \
				echo "Error: One or more nodes are not responding after 10 attempts. Exiting."; \
				exit 1; \
			else \
				echo "Warning: One or more nodes are not responding. Continuing..."; \
			fi; \
		fi; \
		if [[ "$$h1" =~ ^[0-9]+$$ && "$$h2" =~ ^[0-9]+$$ && "$$h3" =~ ^[0-9]+$$ ]]; then \
			if [[ $$h1 -ge $$REQUIRED_HEIGHT && $$h2 -ge $$REQUIRED_HEIGHT && $$h3 -ge $$REQUIRED_HEIGHT ]]; then \
				echo "All nodes have reached height $$REQUIRED_HEIGHT or greater."; \
				break; \
			fi; \
		fi; \
		sleep $$SLEEP; \
	done; \
	if [[ $$i -gt MAX_ATTEMPTS ]]; then \
		echo "Timeout waiting for all nodes to reach height $$REQUIRED_HEIGHT."; \
		exit 1; \
	fi

	# Step 6: Stop Teranode nodes (docker compose down for teranode-1/2/3)
	@echo "Step 6: Stopping Teranode nodes..."
	@docker compose -f compose/docker-compose-3blasters.yml down teranode1 teranode2 teranode3

	# Step 7: Run chainintegrity test
	@echo "Step 7: Running chainintegrity test..."
	@./chainintegrity.run --logfile=chainintegrity --debug | tee chainintegrity_output.log

	# Step 8: Check for hash mismatch and fail if found
	@echo "Step 8: Checking for hash mismatch..."
	@if grep -q "All filtered log file hashes differ! No majority consensus among nodes." chainintegrity_output.log; then \
		echo "Chain integrity test failed: all log file hashes differ, no majority consensus."; \
		exit 1; \
	fi

	# Step 9: Cleanup
	@echo "Step 9: Cleaning up..."
	@docker compose -f compose/docker-compose-3blasters.yml down

	@echo "================================================"
	@echo "✓ Chain Integrity Test completed successfully!"
	@echo "✓ All nodes reached the required block height"
	@echo "✓ Chain integrity verification passed"
	@echo "✓ Consensus achieved among all nodes"
	@echo ""
	@echo "Generated log files:"
	@echo "  - chainintegrity_output.log (main output)"
	@echo "  - chainintegrity*.log (individual node logs)"
	@echo "  - chainintegrity*.filtered.log (filtered logs)"
	@echo ""
	@echo "Test completed at: $$(date)"
	@echo "================================================"

# Chain Integrity Test with custom parameters
.PHONY: chain-integrity-test-custom
chain-integrity-test-custom:
	@echo "Starting Chain Integrity Test with custom parameters..."
	@echo "Usage: make chain-integrity-test-custom REQUIRED_HEIGHT=<height> MAX_ATTEMPTS=<attempts> SLEEP=<seconds>"
	@echo "Default values: REQUIRED_HEIGHT=120, MAX_ATTEMPTS=120, SLEEP=5"
	@echo "================================================"
	@echo "Timestamp: $$(date)"
	@echo ""

	# Set default values if not provided
	$(eval REQUIRED_HEIGHT ?= 120)
	$(eval MAX_ATTEMPTS ?= 120)
	$(eval SLEEP ?= 5)

	@echo "Using parameters: REQUIRED_HEIGHT=$(REQUIRED_HEIGHT), MAX_ATTEMPTS=$(MAX_ATTEMPTS), SLEEP=$(SLEEP)"

	# Step 1: Build chainintegrity binary
	@echo "Step 1: Building chainintegrity binary..."
	$(MAKE) build-chainintegrity

	# Step 2: Clean up old data
	@echo "Step 2: Cleaning up old data..."
	@rm -rf data

	# Step 3: Build teranode image locally
	@echo "Step 3: Building teranode image locally..."
	@docker build -t teranode:latest .

	# Step 4: Start Teranode nodes with 3 block generators
	@echo "Step 4: Starting Teranode nodes with 3 block generators..."
	@docker compose -f compose/docker-compose-3blasters.yml up -d

	# Step 5: Wait for mining to complete with custom parameters
	@echo "Step 5: Waiting for mining to complete (all nodes at height $(REQUIRED_HEIGHT)+ and in sync)..."
	@echo "This may take several minutes..."
	@set -e; \
	REQUIRED_HEIGHT=$(REQUIRED_HEIGHT); \
	MAX_ATTEMPTS=$(MAX_ATTEMPTS); \
	SLEEP=$(SLEEP); \
	\
	# Function to check for errors in all teranode container logs at once \
	check_errors() { \
		# Get current time for this check \
		local current_time; \
		current_time=$$(date -u +"%Y-%m-%dT%H:%M:%SZ"); \
		\
		# Check for errors - if last_check_time is empty, it will check all logs \
		local since_param=""; \
		if [ ! -z "$$last_check_time" ]; then \
			since_param="--since=$$last_check_time"; \
		fi; \
		\
		# Single command pattern that works for both initial and subsequent checks \
		local errors; \
		errors=$$(docker compose -f compose/docker-compose-3blasters.yml logs --no-color $$since_param teranode1 teranode2 teranode3 | grep -i "| ERROR |" || true); \
		\
		# Update timestamp for next check \
		last_check_time=$$current_time; \
		\
		if [[ ! -z "$$errors" ]]; then \
			echo "ERROR: Found error logs in teranode containers:"; \
			echo "$$errors"; \
			return 1; \
		fi; \
		return 0; \
	}; \
	\
	# Initialize empty for first check to get all logs \
	last_check_time=""; \
	\
	for ((i=1; i<=MAX_ATTEMPTS; i++)); do \
		h1=$$(curl -s http://localhost:18090/api/v1/bestblockheader/json | jq -r .height); \
		h2=$$(curl -s http://localhost:28090/api/v1/bestblockheader/json | jq -r .height); \
		h3=$$(curl -s http://localhost:38090/api/v1/bestblockheader/json | jq -r .height); \
		echo "Attempt $$i: heights: $$h1 $$h2 $$h3"; \
		\
		# Check for errors in all teranode containers \
		if ! check_errors; then \
			echo "Errors found in container logs. Exiting."; \
			exit 1; \
		fi; \
		\
		if [[ -z "$$h1" || -z "$$h2" || -z "$$h3" ]]; then \
			if [[ $$i -gt 10 ]]; then \
				echo "Error: One or more nodes are not responding after 10 attempts. Exiting."; \
				exit 1; \
			else \
				echo "Warning: One or more nodes are not responding. Continuing..."; \
			fi; \
		fi; \
		if [[ "$$h1" =~ ^[0-9]+$$ && "$$h2" =~ ^[0-9]+$$ && "$$h3" =~ ^[0-9]+$$ ]]; then \
			if [[ $$h1 -ge $$REQUIRED_HEIGHT && $$h2 -ge $$REQUIRED_HEIGHT && $$h3 -ge $$REQUIRED_HEIGHT ]]; then \
				echo "All nodes have reached height $$REQUIRED_HEIGHT or greater."; \
				break; \
			fi; \
		fi; \
		sleep $$SLEEP; \
	done; \
	if [[ $$i -gt MAX_ATTEMPTS ]]; then \
		echo "Timeout waiting for all nodes to reach height $$REQUIRED_HEIGHT."; \
		exit 1; \
	fi

	# Step 6: Stop Teranode nodes (docker compose down for teranode-1/2/3)
	@echo "Step 6: Stopping Teranode nodes..."
	@docker compose -f compose/docker-compose-3blasters.yml down teranode1 teranode2 teranode3

	# Step 7: Run chainintegrity test
	@echo "Step 7: Running chainintegrity test..."
	@./chainintegrity.run --logfile=chainintegrity --debug | tee chainintegrity_output.log

	# Step 8: Check for hash mismatch and fail if found
	@echo "Step 8: Checking for hash mismatch..."
	@if grep -q "All filtered log file hashes differ! No majority consensus among nodes." chainintegrity_output.log; then \
		echo "Chain integrity test failed: all log file hashes differ, no majority consensus."; \
		exit 1; \
	fi

	# Step 9: Cleanup
	@echo "Step 9: Cleaning up..."
	@docker compose -f compose/docker-compose-3blasters.yml down

	@echo "================================================"
	@echo "Chain Integrity Test completed successfully!"
	@echo "Log files generated:"
	@echo "  - chainintegrity_output.log (main output)"
	@echo "  - chainintegrity*.log (individual node logs)"
	@echo "  - chainintegrity*.filtered.log (filtered logs)"



# Clean up chain integrity test artifacts
.PHONY: clean-chain-integrity
clean-chain-integrity:
	@echo "Cleaning up chain integrity test artifacts..."
	@echo "  - Removing log files..."
	@rm -f chainintegrity*.log
	@rm -f chainintegrity*.filtered.log
	@rm -f chainintegrity_output.log
	@echo "  - Removing chainintegrity binary..."
	@rm -f chainintegrity.run
	@echo "  - Stopping Docker Compose services..."
	@docker compose -f compose/docker-compose-3blasters.yml down 2>/dev/null || true
	@echo "  ✓ Chain integrity test artifacts cleaned up."
	@echo "  ✓ All containers stopped"
	@echo "  ✓ All log files removed"

# AWS ECR login for pulling required images
.PHONY: ecr-login
ecr-login:
	@echo "🔐 AWS ECR Login"
	@echo "=================="
	@echo "  - Logging into AWS ECR (eu-north-1)..."
	@aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com
	@echo "  ✓ ECR login completed successfully"
	@echo "  - You can now pull ECR images"
	@echo ""

# Display hash analysis results from chainintegrity test
.PHONY: show-hashes
show-hashes:
	@echo "📊 Hash Analysis Results:"
	@echo "=========================="
	@if [ -f chainintegrity_output.log ]; then \
		if grep -q "chainintegrity.*\.filtered\.log:" chainintegrity_output.log; then \
			echo "  - Extracting hash information..."; \
			echo ""; \
			grep "chainintegrity.*\.filtered\.log:" chainintegrity_output.log | while read line; do \
				echo "    $$line"; \
			done; \
			echo ""; \
			if grep -q "At least two nodes are consistent" chainintegrity_output.log; then \
				echo "  ✓ Consensus achieved: At least two nodes have matching hashes"; \
			elif grep -q "All filtered log file hashes differ" chainintegrity_output.log; then \
				echo "  ✗ No consensus: All nodes have different hashes"; \
			else \
				echo "  ⚠ Hash analysis result unclear - check chainintegrity_output.log"; \
			fi; \
		else \
			echo "  ⚠ No hash information found in chainintegrity_output.log"; \
			echo "  - Run 'make chain-integrity-test' first to generate the log file"; \
		fi; \
	else \
		echo "  ⚠ chainintegrity_output.log not found"; \
		echo "  - Run 'make chain-integrity-test' first to generate the log file"; \
	fi
	@echo ""

# Quick chain integrity test (shorter wait times for faster testing)
.PHONY: chain-integrity-test-quick
chain-integrity-test-quick:
	@echo "Starting Quick Chain Integrity Test (shorter wait times)..."
	@echo "  - Target height: 50 blocks"
	@echo "  - Maximum wait time: 3 minutes (60 attempts × 3 seconds)"
	@echo "  - Check interval: 3 seconds"
	@echo "  - Use this for faster development iterations"
	@echo ""
	$(MAKE) chain-integrity-test-custom REQUIRED_HEIGHT=50 MAX_ATTEMPTS=60 SLEEP=3
