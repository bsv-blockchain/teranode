# 🖥 Developer Setup - Pre-requisites and Installation

This guide assists you in setting up the Teranode project on your machine. The below assumes you are running a recent version of Mac OS.

## Index

1. [Install Go](#1-install-go)
2. [Set Go Environment Variables](#2-set-go-environment-variables)
3. [Python and Dependencies](#3-python-and-dependencies)
    - [3.1 Install Python (via Homebrew)](#31-install-python-via-homebrew)
    - [3.2 (Recommended) Use a Python Virtual Environment to install PyYAML](#32-recommended-use-a-python-virtual-environment-to-install-pyyaml)
    - [3.3 Install Dependencies Within the Virtual Environment](#33-install-dependencies-within-the-virtual-environment)
    - [3.4 Verify Installation](#34-verify-installation)
    - [Alternative: Use pipx for CLI tools - NOT recommended for Teranode Development](#alternative-use-pipx-for-cli-tools---not-recommended-for-teranode-development)
4. [Clone the Project and Install Dependencies](#4-clone-the-project-and-install-dependencies)
5. [Configure Settings](#5-configure-settings)
    - [5.1 Introducing developer-specific settings in `settings_local.conf`](#51-introducing-developer-specific-settings-in-settings_localconf)
    - [5.3 Verify](#53-verify)
6. [Prerequisites for Running the Node](#6-prerequisites-for-running-the-node)
    - [6.1 Install OrbStack](#61-install-orbstack)
    - [6.2 Start PostgreSQL](#62-start-postgresql)
7. [Run the Node](#7-run-the-node)
    - [7.2 Debugging Teranode](#72-debugging-teranode)
8. [Troubleshooting](#8-troubleshooting)
    - [8.1. Dependency errors and conflicts](#81-dependency-errors-and-conflicts)
    - [Next Steps](#next-steps)

## 1. Install Go

Download and install the latest version of Go. As of October 2025, it's `1.25.2`.

[Go Installation Guide](https://go.dev/doc/install)

**Test Installation**:

Open a new terminal and execute:

```bash
go version
```

It should display `go1.25.2` or above.

## 2. Set Go Environment Variables

Add these lines to `.zprofile` or `.bash_profile`, depending on which one your development machine uses:

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
export GOPATH="$(go env GOPATH)"
export GOBIN="$(go env GOPATH)/bin"
```

**Test Configuration**:

Open a new terminal and execute:

```bash
echo $GOPATH
echo $GOBIN
```

Both should display paths related to Go.

## 3. Python and Dependencies

### 3.1 Install Python (via Homebrew)

```bash
brew install python
```

By default, Homebrew will install Python 3.x and create symlinks like `python3` and `pip3`. You can optionally create a symlink for `python` and `pip` if you want shorter commands (but check if they already exist first):

```bash
ln -s /opt/homebrew/bin/python3 /opt/homebrew/bin/python  # might already exist
ln -s /opt/homebrew/bin/pip3 /opt/homebrew/bin/pip        # might already exist
```

### 3.2 (Recommended) Use a Python Virtual Environment to install PyYAML

Because of [PEP 668](https://peps.python.org/pep-0668/) and Homebrew’s “externally-managed-environment” setup, you can’t do `pip install ...` directly into the system-wide Python.

**Instead, create and activate a virtual environment**:

```bash
python3 -m venv ~/my_python_env     # choose any path you like
source ~/my_python_env/bin/activate
```

After activating, your shell should show something like `(my_python_env)` as a prefix.

### 3.3 Install Dependencies Within the Virtual Environment

Once your virtual environment is active, you can safely use `pip` to install packages **without** system conflicts:

```bash
python -m pip install --upgrade pip
pip install PyYAML
```

### 3.4 Verify Installation

```bash
python -c "import yaml; print(yaml.__version__)"
```

This should print out the installed PyYAML version (e.g., `6.0.2` or similar).

#### Alternative: Use `pipx` (for CLI tools) - NOT recommended for Teranode Development

If you need PyYAML as part of a **standalone command-line tool**, you could use [pipx](https://pypa.github.io/pipx/) instead:

```bash
brew install pipx
pipx install PyYAML
```

However, most Teranode workflows will need PyYAML as a library for scripts, so a virtual environment is usually best.

## 4. Clone the Project and Install Dependencies

Clone the project:

```bash
git clone git@github.com:bsv-blockchain/teranode.git
```

**Install all dependencies**:

Execute:

```bash
cd teranode

# This will install all required dependencies (protobuf, golangci-lint, etc.)
make install
```

> Note:
> If you receive an error `ModuleNotFoundError: No module named 'yaml'` error, refer to this [issue](https://github.com/yaml/pyyaml/issues/291) for a potential fix. Example:
>
> ```bash
> PYTHONPATH=$HOME/Library/Python/3.9/lib/python/site-packages make install  #Make sure the path is correct for your own python version
> ```

## 5. Configure Settings

Teranode uses two configuration files:

- `settings.conf` - Contains sensible defaults for all environments. You should NOT modify this file as part of the scope of this guide.
- `settings_local.conf` - Contains developer-specific and deployment-specific settings

### 5.1 Introducing developer-specific settings in `settings_local.conf`

1. In your project directory, create a file `settings_local.conf`. This file is used for your personal development settings, and it's not tracked in source control.

2. Introduce any settings override in `settings_local.conf` that you might require for your development. Use the `settings.conf` as a reference for common settings and their default values.

3. The settings you are adding will use a prefix (settings context) that identifies your context. By default, your settings context should be `dev.`. You can further refine this by using a more specific prefix, such as `dev.john` or `dev.me`. However, it is recommended to use the default prefix `dev.`, and only refine it in very specific cases.

In order for your node to read **your** custom lines, you set `SETTINGS_CONTEXT` to match the prefix you used (i.e., `dev`).

In **zsh**, open `~/.zprofile` (or `~/.zshrc`). In **bash**, open `~/.bash_profile` (or `~/.bashrc`).

Add:

```bash
export SETTINGS_CONTEXT=dev
```

(If you have used a richer prefix, such as `dev.john`, you would set `SETTINGS_CONTEXT=dev.john`)

After editing, **reload** your shell config:

```bash
source ~/.zprofile
```

(or the equivalent for your shell).

### 5.3 Verify

1. **Echo** the environment variable to ensure it's set correctly:

    ```bash
    echo $SETTINGS_CONTEXT
    ```

    Should print `dev`.

2. **Run** or **restart** your node. Check logs or console output to confirm it's picking up the lines with `dev`.

## 6. Prerequisites for Running the Node

### 6.1 Install OrbStack

Teranode uses Docker containers for running dependencies like Kafka and PostgreSQL. For Mac developers, we recommend using [OrbStack](https://orbstack.dev/) - a fast, lightweight Docker Desktop alternative optimized for macOS.

**Why OrbStack?**

- **Faster**: 2-3x faster than Docker Desktop for container startup and file operations
- **Lighter**: Uses significantly less CPU and memory
- **Native**: Built specifically for macOS with better integration
- **Compatible**: Drop-in replacement for Docker Desktop - all Docker commands work the same

**Installation**:

1. Download OrbStack from [orbstack.dev](https://orbstack.dev/)
2. Open the downloaded file and drag OrbStack to your Applications folder
3. Launch OrbStack from your Applications folder
4. Follow the brief setup wizard

**Verify installation**:

```bash
docker --version
```

### 6.2 Start PostgreSQL

Once OrbStack is installed and running, start PostgreSQL with:

```bash
# Start PostgreSQL in Docker
./scripts/postgres.sh
```

> **Note on Kafka**: Development mode uses in-memory Kafka by default (no setup required). For production-like testing with Docker-based Kafka, see [Kafka Settings Reference](../../references/settings/kafka_settings.md). If you configure your settings to use Aerospike for UTXO storage, you'll also need to run the Aerospike script:
>
> ```bash
> # Start Aerospike in Docker
> ./scripts/aerospike.sh
> ```

## 7. Run the Node

You can run the entire node with the following command:

```bash
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run .
```

If no errors are seen, you have successfully installed the project and are ready to start working on the project or running the node.

Note that the node is initialized in IDLE mode by default. You'll need to transition it to RUNNING mode to start processing transactions.

### 7.1. Executing the Teranode-CLI as a Developer

The Teranode-CLI allows you to interact with Teranode services. You can use it to transition the node to different states, query its current state, and perform various maintenance operations. For a comprehensive guide on using the Teranode-CLI as a developer, see the [Developer's Guide to Teranode-CLI](../../howto/developersHowToTeranodeCLI.md).

#### Building the Teranode-CLI

Build the Teranode-CLI tool with:

```bash
go build -o teranode-cli ./cmd/teranodecli
```

#### Executing Commands

Once built, you can run commands directly:

```bash
# Get current FSM state
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli getfsmstate

# Set FSM state to RUNNING
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] ./teranode-cli setfsmstate --fsmstate running
```

#### Available Commands

The Teranode-CLI provides several commands:

| Command              | Description                                |
|----------------------|--------------------------------------------|
| `getfsmstate`        | Get the current FSM State                  |
| `setfsmstate`        | Set the FSM State (with `--fsmstate` flag) |
| `settings`           | View system configuration                  |
| `aerospikereader`    | Read transaction data from Aerospike       |
| `filereader`         | Read and process files                     |
| `seeder`             | Seed initial blockchain data               |
| `bitcointoutxoset`   | Convert Bitcoin data to UTXO set           |
| `utxopersister`      | Manage UTXO persistence                    |
| `export-blocks`      | Export blockchain to CSV                   |
| `import-blocks`      | Import blockchain from CSV                 |
| `checkblocktemplate` | Check block template                       |

#### Getting Help

For general help and a list of available commands:

```bash
# General help
./teranode-cli

# Command-specific help
./teranode-cli setfsmstate --help
```

The CLI will use your development settings as specified by your `SETTINGS_CONTEXT` environment variable.

#### Transitioning the Node to RUNNING Mode

To transition the node from IDLE to RUNNING mode, use:

```bash
# Get current FSM state
SETTINGS_CONTEXT=dev ./teranode-cli getfsmstate

# Set FSM state to RUNNING
SETTINGS_CONTEXT=dev ./teranode-cli setfsmstate --fsmstate running
```

After executing these commands, your log should show a successful transition:

```bash
[Blockchain Client] FSM successfully transitioned from IDLE to state:RUNNING
```

### 7.2. Debugging Teranode

Teranode supports debugging using Delve, the Go debugger. You can use any IDE that supports Delve, including VS Code, GoLand, or the Delve CLI directly.

#### Local Development Debugging

To debug Teranode during local development:

1. **Build with debug symbols**:

    ```bash
    DEBUG=true make build
    ```

    This enables debug flags (`-N -l`) that disable optimizations and inlining, making debugging easier.

2. **Attach your debugger** to the running Teranode process using your preferred IDE or tool.

#### Remote Debugging (Kubernetes Deployments)

For debugging Teranode running in Kubernetes environments:

- See the [Remote Debug Guide](../../howto/howToRemoteDebugTeranode.md) for detailed instructions on:

    - Configuring the Kubernetes cluster for remote debugging
    - Port forwarding the debugger
    - Connecting with VS Code, GoLand, or Delve CLI
    - Debugging multiple services simultaneously

---

## 8. Troubleshooting

### 8.1. Dependency errors and conflicts

Should you have errors with dependencies, try running the following commands:

```bash
go clean -cache
go clean -modcache
rm go.sum
go mod tidy
go mod download
```

This will effectively reset your cache and re-download all dependencies.

## Next Steps

- [Check our Git Commit Signing Setup Guide for Contributors](../../references/gitCommitSigningSetupGuide.md)
