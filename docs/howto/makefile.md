# 🛠️ Makefile Documentation

This Makefile facilitates a variety of development and build tasks for the Teranode project.

- [🛠️ Makefile Documentation](#️-makefile-documentation)
  - [Environment Configuration](#environment-configuration)
  - [Key Commands:](#key-commands)
  - [All Commands:](#all-commands)
    - [General Configuration:](#general-configuration)
    - [Setting Debug Flags:](#setting-debug-flags)
    - [Build All Components:](#build-all-components)
    - [Dependencies:](#dependencies)
    - [Development:](#development)
    - [Building:](#building)
    - [Testing:](#testing)
    - [Code Generation:](#code-generation)
    - [Cleanup:](#cleanup)
    - [Linting and Static Analysis:](#linting-and-static-analysis)
    - [Installation:](#installation)
  - [Notes:](#notes)



## Environment Configuration
- `SHELL`: Indicates the shell for the make process. Set to `/bin/bash`.
- `DEBUG_FLAGS`: Flags to use in debug mode.

## Key Commands:

1. **set_debug_flags**: Configures debug flags based on the `DEBUG` variable.
2. **all**: Executes the following tasks in order: `deps`, `install`, `lint`, `build`, and `test`.
3. **deps**: Downloads required Go modules.
4. **dev**: Runs both the `dev-dashboard` and `dev-teranode` concurrently.
5. **dev-teranode**: Executes the Go project.
6. **dev-dashboard**: Installs and runs the Node.js dashboard project located in `./ui/dashboard`.
7. **build**: Builds the Teranode application and dashboard.
8. **test**: Executes Go tests with race detection.
9. **sequentialtests**: Runs tests sequentially for more stability.
10. **smoketests**: Runs smoke tests for RPC functionality.
11. **gen**: Generates required Go code from `.proto` files for various services.
12. **clean_gen**: Removes generated Go files.
13. **clean**: Cleans up generated binaries and build artifacts.
14. **install-lint**: Installs linting tools (golangci-lint and staticcheck).
15. **lint**: Executes lint checks.
16. **install**: Installs all development dependencies including: golangci-lint, staticcheck, protobuf, protoc-gen-go, protoc-gen-go-grpc, libtool, autoconf, automake, and pre-commit hooks.

## All Commands:

### General Configuration:

- `SHELL=/bin/bash`: Sets the shell to bash for running commands.
- `DEBUG_FLAGS=`: Initializes an empty variable for debug flags.

### Setting Debug Flags:

- `set_debug_flags`: Sets the debug flags if the `DEBUG` environment variable is set to `true`.

### Build All Components:

- `all`: A composite task that runs `deps`, `install`, `lint`, `build`, and `test`.

### Dependencies:

- `deps`: Downloads necessary Go modules.

### Development:

- `dev`: Runs both `dev-dashboard` and `dev-teranode`.
- `dev-teranode`: Executes the main Go project.
- `dev-dashboard`: Installs and runs a Node.js project located in `./ui/dashboard`.

### Building:

- `build`: A composite task that runs `update_config`, `build-teranode-with-dashboard`, `build-teranode-cli`, and `clean_backup`.
- `update_config`: Updates the configuration based on provided parameters.
- `clean_backup`: Removes backup configuration files.
- `build-teranode-with-dashboard`: Builds Teranode with the dashboard UI integrated.
- `build-teranode`: Builds the main Teranode project with race detection and specific tags.
- `build-teranode-no-debug`: Builds Teranode without debug symbols for production use.
- `build-teranode-ci`: Builds Teranode for continuous integration environments.
- `build-teranode-cli`: Builds the Teranode CLI tool.
- `build-dashboard`: Installs npm dependencies and builds the dashboard UI.

Other specific build tasks for components:

- `build-chainintegrity`: Builds the chain integrity component.
- `build-tx-blaster`: Builds the transaction blaster component.
- `build-blockchainstatus`: Builds the blockchain status monitoring component.

### Testing:

- `test`: Runs unit tests with race detection and configurable output format.
- `buildtest`: Builds tests without running them for separate execution.
- `sequentialtest`: Executes tests sequentially for more stable results.
- `longtest`: Executes long tests.
- `nightly-tests`: Runs comprehensive tests typically scheduled for nightly builds.
- `smoketests`: Runs smoke tests focused on RPC functionality.
- `install-tools`: Installs testing tools like the CTRF JSON reporter.

### Code Generation:

- `gen`: Generates Go code from Protocol Buffers for various services.

### Cleanup:

- `clean_gen`: Removes generated Go code.
- `clean`: Cleans up built artifacts.

### Linting and Static Analysis:

- `install-lint`: Installs tools for linting and static analysis (golangci-lint and staticcheck).
- `lint`: Runs linters to check changed files compared to main branch.
- `lint-new`: Checks only unstaged/untracked changes, useful for quick validation.
- `lint-full`: Runs linters on the entire codebase.
- `lint-full-changed-dirs`: Runs linters on changed directories only.

### Installation:

- `install`: Comprehensive installation command that installs:
  - Linting tools (golangci-lint and staticcheck)
  - Protocol Buffers compiler and tools (protobuf, protoc-gen-go, protoc-gen-go-grpc)
  - Build tools (libtool, autoconf, automake)
  - Git hooks via pre-commit

  This is the preferred command for setting up a new development environment, as it installs all necessary dependencies.

## Notes:
- The `PHONY` declarations before each command indicate that they do not produce or depend on any files, ensuring that the associated command is executed every time it's called.
- The Makefile includes a combination of Go build commands, Node.js commands, and various tooling setups.
- There's extensive use of conditional debug flags and platform-specific build tags.
- Several configuration variables are available like `DEBUG` for debug builds, `TXMETA_TAG` for transaction metadata cache configuration, and `LOCAL_TEST_START_FROM_STATE` for testing.
- The `generate_fsm_diagram` target creates visual state machine diagrams for the blockchain service.
- Some targets are commented out in the Makefile (like propagation-blaster, utxostore-blaster) but may be re-enabled as needed.
