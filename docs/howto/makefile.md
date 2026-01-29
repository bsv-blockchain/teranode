# Makefile Documentation

This Makefile facilitates a variety of development and build tasks for the Teranode project.

- [Makefile Documentation](#makefile-documentation)
    - [Environment Configuration](#environment-configuration)
    - [Key Commands](#key-commands)
    - [All Commands](#all-commands)
        - [General Configuration](#general-configuration)
        - [Setting Debug Flags](#setting-debug-flags)
        - [Setting Transaction Metadata Cache Flags](#setting-transaction-metadata-cache-flags)
        - [Build All Components](#build-all-components)
        - [Dependencies](#dependencies)
        - [Development](#development)
        - [Building](#building)
        - [Building Tools and Utilities](#building-tools-and-utilities)
        - [Testing](#testing)
        - [Chain Integrity Testing](#chain-integrity-testing)
        - [Code Generation](#code-generation)
        - [Cleanup](#cleanup)
        - [Linting and Static Analysis](#linting-and-static-analysis)
        - [Installation](#installation)
        - [Utilities](#utilities)
    - [Environment Variables](#environment-variables)
    - [Usage Examples](#usage-examples)
    - [Notes](#notes)

## Environment Configuration

- `SHELL`: Indicates the shell for the make process. Set to `/bin/bash`.
- `DEBUG_FLAGS`: Flags to use in debug mode.
- `TXMETA_TAG`: Tag for transaction metadata cache configuration.
- `SETTINGS_CONTEXT_DEFAULT`: Default settings context (set to `docker.ci`).
- `LOCAL_TEST_START_FROM_STATE`: Optional parameter to configure test start state.

## Key Commands

**Type**: Target | **Description**: Description | **Impact**: What it does

- **all**: Executes the following tasks in order: `deps`, `install`, `lint`, `build`, and `test`.
- **deps**: Downloads required Go modules.
- **install**: Installs all development dependencies including linting tools, protobuf tools, build tools, and git hooks.
- **dev**: Runs both the `dev-dashboard` and `dev-teranode` concurrently.
- **dev-teranode**: Executes the Go project.
- **dev-dashboard**: Installs and runs the Node.js dashboard project located in `./ui/dashboard`.
- **build**: Builds the Teranode application and dashboard.
- **build-teranode**: Builds the main Teranode binary without the dashboard.
- **build-teranode-ci**: Builds Teranode with race detection for CI environments.
- **test**: Executes Go tests with race detection (excludes `test/` directory).
- **testall**: Runs all tests: `test`, `longtest`, and `sequentialtest`.
- **gen**: Generates required Go code from `.proto` files for various services.
- **clean**: Cleans up generated binaries and build artifacts.
- **lint**: Executes lint checks on changed files compared to main branch.
- **chain-integrity-test**: Runs comprehensive chain integrity test with 3 nodes.

## All Commands

### General Configuration

- `SHELL=/bin/bash`: Sets the shell to bash for running commands.
- `DEBUG_FLAGS=`: Initializes an empty variable for debug flags.
- `TXMETA_TAG=`: Initializes transaction metadata cache tag.
- `SETTINGS_CONTEXT_DEFAULT := docker.ci`: Sets default settings context.

### Setting Debug Flags

- **set_debug_flags**: Sets the debug flags (`-N -l`) if the `DEBUG` environment variable is set to `true`.

### Setting Transaction Metadata Cache Flags

- **set_txmetacache_flag**: Sets the `TXMETA_TAG` based on environment variables:

    - `TXMETA_SMALL_TAG=true`: Uses `smalltxmetacache`
    - `TXMETA_TEST_TAG=true`: Uses `testtxmetacache`
    - Default: Uses `largetxmetacache`

### Build All Components

- **all**: A composite task that runs `deps`, `install`, `lint`, `build`, and `test`.

### Dependencies

- **deps**: Downloads necessary Go modules using `go mod download`.

### Development

- **dev**: Runs both `dev-dashboard` and `dev-teranode` concurrently.
- **dev-teranode**: Executes the main Go project with `go run .`.
- **dev-dashboard**: Installs npm dependencies and runs the Node.js project in development mode at `./ui/dashboard`.

### Building

- **build**: A composite task that runs `update_config`, `build-teranode-with-dashboard`, `build-teranode-cli`, and `clean_backup`.
- **update_config**: Updates the `settings_local.conf` configuration file based on `LOCAL_TEST_START_FROM_STATE` parameter.
- **clean_backup**: Removes backup configuration files (`settings_local.conf.bak`).
- **build-teranode-with-dashboard**: Builds Teranode with the dashboard UI integrated. Includes debug flags and transaction metadata cache configuration.
- **build-teranode**: Builds the main Teranode project with specific build tags (`aerospike`, transaction metadata cache tag).
- **build-teranode-no-debug**: Builds Teranode without debug symbols for production use (stripped binary).
- **build-teranode-ci**: Builds Teranode for continuous integration environments with race detection enabled.
- **build-teranode-cli**: Builds the Teranode CLI tool at `./cmd/teranodecli`.
- **build-dashboard**: Installs npm dependencies and builds the dashboard UI for production.

### Building Tools and Utilities

- **build-chainintegrity**: Builds the chain integrity testing tool at `./compose/cmd/chainintegrity/`.
- **build-tx-blaster**: Builds the transaction blaster performance testing tool at `./cmd/txblaster/`.
- **build-blockchainstatus**: Builds the blockchain status utility at `./cmd/blockchainstatus/`.

### Testing

- **test**: Runs unit tests with race detection and configurable output format. Excludes tests in the `test/` directory. Uses `testtxmetacache` tag and `SETTINGS_CONTEXT=test`.
- **buildtest**: Builds tests without running them for separate execution.
- **sequentialtest**: Executes tests in the `test/sequentialtest/` directory sequentially for more stable results.
- **longtest**: Executes long-running tests in the `test/longtest/` directory with 5-minute timeout.
- **testall**: Runs all test suites: `test`, `longtest`, and `sequentialtest`.
- **nightly-tests**: Runs comprehensive tests typically scheduled for nightly builds. Builds Docker images and uses CTRF JSON reporter for results.
- **smoketest**: Runs smoke tests in the `test/e2e/daemon/ready/` directory focused on basic functionality.
- **install-tools**: Installs testing tools like the CTRF JSON reporter.

### Chain Integrity Testing

The chain integrity test suite validates that multiple Teranode instances maintain consensus and produce identical blockchain states.

- **chain-integrity-test**: Full chain integrity test that:

    - Builds the chainintegrity binary
    - Cleans up old test data
    - Builds Docker image for teranode
    - Starts 3 teranode instances with block generators
    - Waits for all nodes to reach height 120+ and synchronize
    - Validates chain integrity by comparing node states
    - Checks for error logs during execution
    - Target: 120 blocks, Max wait: 10 minutes (120 attempts × 5 seconds)

- **chain-integrity-test-custom**: Chain integrity test with customizable parameters:

    - Usage: `make chain-integrity-test-custom REQUIRED_HEIGHT=<height> MAX_ATTEMPTS=<attempts> SLEEP=<seconds>`
    - Default values: `REQUIRED_HEIGHT=120`, `MAX_ATTEMPTS=120`, `SLEEP=5`
    - Allows testing different block heights and wait times

- **chain-integrity-test-quick**: Quick chain integrity test for faster development iterations:

    - Target: 50 blocks
    - Max wait: 3 minutes (60 attempts × 3 seconds)
    - Check interval: 3 seconds
    - Use this for rapid testing during development

- **clean-chain-integrity**: Cleans up chain integrity test artifacts:

    - Removes all chainintegrity log files
    - Removes chainintegrity binary
    - Stops Docker Compose services
    - Use this to reset between test runs

- **ecr-login**: AWS ECR login for pulling required Docker images:

    - Logs into AWS ECR (eu-north-1 region)
    - Required before building or pulling ECR-hosted images
    - Uses AWS CLI credentials

- **show-hashes**: Displays hash analysis results from chainintegrity test:

    - Extracts hash information from test output
    - Shows consensus status among nodes
    - Useful for debugging chain integrity issues

### Code Generation

- **gen**: Generates Go code from Protocol Buffers for various services:

    - `model/model.proto`
    - `errors/error.proto`
    - `stores/utxo/status.proto`
    - `services/validator/validator_api/validator_api.proto`
    - `services/propagation/propagation_api/propagation_api.proto`
    - `services/blockassembly/blockassembly_api/blockassembly_api.proto`
    - `services/blockvalidation/blockvalidation_api/blockvalidation_api.proto`
    - `services/subtreevalidation/subtreevalidation_api/subtreevalidation_api.proto`
    - `services/blockchain/blockchain_api/blockchain_api.proto`
    - `services/asset/asset_api/asset_api.proto`
    - `services/alert/alert_api/alert_api.proto`
    - `services/legacy/peer_api/peer_api.proto`
    - `services/p2p/p2p_api/p2p_api.proto`
    - `util/kafka/kafka_message/kafka_messages.proto`

### Cleanup

- **clean_gen**: Removes generated Go code (`.pb.go` files) from:

    - All service API directories
    - Model and error directories
    - UTXO store directory

- **clean**: Cleans up built artifacts:

    - Removes `.tar.gz` archives
    - Removes binary executables (`blaster.run`, `blockchainstatus.run`)
    - Removes build directory
    - Removes coverage output files

### Linting and Static Analysis

- **install-lint**: Installs tools for linting and static analysis:

    - `golangci-lint` (comprehensive Go linter)
    - `staticcheck` (static analysis tool)

- **lint**: Runs linters to check changed files compared to main branch:

    - Fetches latest `origin/main`
    - Shows new linting errors/warnings introduced by your changes
    - Only checks files modified since branching from main

- **lint-new**: Checks only unstaged/untracked changes:

    - Useful for quick validation during development
    - Falls back to checking last commit if no uncommitted changes
    - Faster than `lint` for incremental work

- **lint-full**: Runs linters on the entire codebase:

    - Shows all lint errors and warnings across all files
    - Use before major releases or when refactoring

- **lint-full-changed-dirs**: Runs linters on changed directories only:

    - Checks all files in directories containing `.go` file changes
    - More comprehensive than `lint` but faster than `lint-full`
    - Shows all errors in modified directories, not just new ones

### Installation

- **install**: Comprehensive installation command that sets up a complete development environment:

    - **Quality tools**: golangci-lint and staticcheck for linting
    - **Core dependencies**: Protocol Buffers compiler and Go protoc plugins (protobuf, protoc-gen-go, protoc-gen-go-grpc)
    - **Build dependencies**: Build tools for native code components (libtool, autoconf, automake)
    - **Workflow tools**: pre-commit hooks for team collaboration

    This is the preferred command for setting up a new development environment, as it installs all necessary dependencies.

### Utilities

- **reset-data**: Resets test data from archive:

    - Unzips `data.zip`
    - Sets proper permissions on data directory
    - Use when test data becomes corrupted

- **generate_fsm_diagram**: Generates finite state machine diagram:

    - Runs FSM visualizer tool
    - Outputs diagram to `docs/state-machine.diagram.md`
    - Visualizes blockchain service state transitions

## Environment Variables

The Makefile supports several environment variables to customize builds and tests:

**Build Configuration:**

- `DEBUG`: Set to `true` to enable debug flags (`-N -l`) for debugging with delve or other debuggers
    - Example: `DEBUG=true make build`

- `TXMETA_SMALL_TAG`: Set to `true` to use small transaction metadata cache
    - Example: `TXMETA_SMALL_TAG=true make build-teranode`

- `TXMETA_TEST_TAG`: Set to `true` to use test-sized transaction metadata cache
    - Example: `TXMETA_TEST_TAG=true make build-teranode`

- `LOCAL_TEST_START_FROM_STATE`: Configures the starting state for local tests
    - Example: `make build LOCAL_TEST_START_FROM_STATE=genesis`

**Cross-Compilation Configuration:**

- `CGO_ENABLED`: Enable or disable CGO (default: `1`). Set to `0` for pure Go builds without C dependencies.
    - Example: `CGO_ENABLED=0 make build-teranode`

- `GOOS`: Target operating system (default: current OS). Common values: `linux`, `darwin`, `windows`
    - Example: `GOOS=linux make build-teranode`

- `GOARCH`: Target architecture (default: current architecture). Common values: `amd64`, `arm64`
    - Example: `GOARCH=arm64 make build-teranode`

- **Combined example** (build for Linux ARM64, e.g., AWS Graviton):
    - `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 make build-teranode`

**Version Information (auto-detected from git):**

- `GIT_VERSION`: Git version string (auto-detected by `scripts/determine-git-version.sh`)
- `GIT_COMMIT`: Full git commit hash
- `GIT_SHA`: Short git SHA
- `GIT_TAG`: Git tag if on a tagged commit
- `GIT_TIMESTAMP`: Timestamp of the git commit

**Test Configuration:**

- `SETTINGS_CONTEXT`: Settings context for test execution (default: `test` for unit tests, `docker.ci` for integration tests)
    - Example: `SETTINGS_CONTEXT=docker go test ./...`

**Chain Integrity Test Configuration:**

- `REQUIRED_HEIGHT`: Target block height for chain integrity tests (default: 120)
- `MAX_ATTEMPTS`: Maximum polling attempts (default: 120)
- `SLEEP`: Seconds between polling attempts (default: 5)

## Usage Examples

**Development workflow:**

```bash
# First-time setup
make install

# Run in development mode (auto-reload)
make dev

# Run only teranode in development
make dev-teranode
```

**Building:**

```bash
# Build with dashboard (production)
make build

# Build with debug symbols
DEBUG=true make build

# Build for CI with race detection
make build-teranode-ci

# Build with small cache for testing
TXMETA_SMALL_TAG=true make build-teranode
```

**Testing:**

```bash
# Run all unit tests
make test

# Run all test suites (unit + long + sequential)
make testall

# Run only smoke tests
make smoketest

# Run tests in specific directory
go test -v -race -tags "testtxmetacache" -count=1 ./services/validator/...
```

**Chain Integrity Testing:**

```bash
# Run full chain integrity test (120 blocks)
make chain-integrity-test

# Quick test (50 blocks)
make chain-integrity-test-quick

# Custom parameters
make chain-integrity-test-custom REQUIRED_HEIGHT=200 MAX_ATTEMPTS=240 SLEEP=3

# View hash analysis results
make show-hashes

# Clean up after testing
make clean-chain-integrity
```

**Code generation:**

```bash
# Generate Go code from protobuf definitions
make gen

# Clean generated code
make clean_gen
```

**Linting:**

```bash
# Check only your changes vs main
make lint

# Check only uncommitted changes
make lint-new

# Check entire codebase
make lint-full

# Check changed directories fully
make lint-full-changed-dirs
```

**Utilities:**

```bash
# Generate FSM state machine diagram
make generate_fsm_diagram

# Login to AWS ECR
make ecr-login

# Reset test data
make reset-data
```

## Notes

- **PHONY declarations**: All targets are declared as `.PHONY` to indicate that they do not produce or depend on any files. This ensures that the associated command is executed every time it's called, regardless of file timestamps.

- **Go modules**: The Makefile uses `-mod=readonly` for production builds to ensure reproducible builds and prevent accidental dependency changes.

- **Build tags**: Several build commands use tags like `aerospike` and transaction metadata cache tags (`smalltxmetacache`, `testtxmetacache`, `largetxmetacache`) to control conditional compilation.

- **Race detection**: The `-race` flag is only used in CI builds (`build-teranode-ci`) and test commands to detect race conditions. It's not enabled in standard development builds due to performance overhead.

- **Trimpath**: Production builds use `--trimpath` to remove absolute file paths from compiled binaries for reproducibility and security.

- **ldflags**: Build commands inject version information at compile time using `-ldflags` to set `main.commit` and `main.version` variables.

- **Commented-out targets**: Several targets are commented out in the Makefile but may be re-enabled as needed:

    - `build-propagation-blaster`: Propagation performance testing tool
    - `build-utxostore-blaster`: UTXO store performance testing tool
    - `build-s3-blaster`: S3 storage performance testing tool
    - `build-blockassembly-blaster`: Block assembly performance testing tool
    - `build-aerospiketest`: Aerospike database testing utility

- **Platform differences**: The `update_config` target contains macOS-specific `sed` commands (BSD sed). Linux users should uncomment the GNU sed variant.

- **Docker Compose**: Chain integrity tests use `docker-compose-3blasters.yml` to orchestrate multiple Teranode instances for consensus testing.

- **Test isolation**: Unit tests use `SETTINGS_CONTEXT=test` to isolate test configuration from development and production settings.

- **Coverage reports**: Test commands generate `coverage.out` files for code coverage analysis.
