# 🛠️ Makefile Documentation

This Makefile facilitates a variety of development and build tasks for the Teranode project.

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
7. **build**: Builds both the dashboard and Teranode.
8. **test**: Executes Go tests excluding the playground and PoC directories.
9. **longtests**: Executes long-running Go tests.
10. **testall**: Runs linting and long tests.
11. **gen**: Generates required Go code from `.proto` files for various services.
12. **clean_gen**: Removes generated Go files.
13. **clean**: Cleans up generated binaries and build artifacts.
14. **install-lint**: Installs linting tools.
15. **lint**: Executes lint checks.
16. **install**: Installs lint tools, proto generators, and sets up pre-commit hooks.

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

- `build`: A composite task that runs `build-dashboard` and `build-teranode`.
- `build-teranode`: Builds the main Teranode project with certain tags and linker flags.
- `build-dashboard`: Installs npm dependencies and builds a dashboard UI.

Other specific build tasks for different components:

- `build-chainintegrity`: Builds the chain integrity component.
- `build-tx-blaster`: Builds the transaction blaster component.
- `build-propagation-blaster`: Builds the propagation blaster component.
- `build-utxostore-blaster`: Builds the UTXO store blaster component.
- `build-s3-blaster`: Builds the S3 blaster component.
- `build-blockassembly-blaster`: Builds the block assembly blaster component.
- `build-blockchainstatus`: Builds the blockchainstatus component.
- `build-aerospiketest`: Builds the Aerospike test component.

### Testing:

- `test`: Runs unit tests, excluding certain packages.
- `longtests`: Executes long-running tests with coverage.
- `testall`: Lints the codebase and runs long tests.

### Code Generation:

- `gen`: Generates Go code from Protocol Buffers for various services.

### Cleanup:

- `clean_gen`: Removes generated Go code.
- `clean`: Cleans up built artifacts.

### Linting and Static Analysis:

- `install-lint`: Installs necessary tools for linting and static analysis.
- `lint`: Runs linters and static analysis on the codebase.

### Installation:

- `install`: Installs linting tools, Protocol Buffers plugins, and pre-commit hooks.

## Notes:
- The `PHONY` declarations before each command indicate that they do not produce or depend on any files, ensuring that the associated command is executed every time it's called.
- The Makefile includes a combination of Go build commands, Node.js commands, and various tooling setups.
- There's extensive use of conditional debug flags and platform-specific build tags.
