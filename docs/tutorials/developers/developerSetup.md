# 🖥 Developer Setup - Pre-requisites and Installation

This guide assists you in setting up the Teranode project on your machine. The below assumes you are running a recent version of Mac OS.


1. [Install Go](#1-install-go)
2. [Set Go Environment Variables](#2-set-go-environment-variables)
3. [Python & Dependencies](#3-python--dependencies)
- [Python:](#python)
- [Pip (Python Package Installer):](#pip-python-package-installer)
- [PyYAML:](#pyyaml)
4. [Project Dependencies](#4-project-dependencies)
5. [Build & Install secp256k1](#5-build--install-secp256k1)
6. [Clone the Project and Install Additional Dependencies](#6-clone-the-project-and-install-additional-dependencies)
7. [Configure Your Node Dev Settings](#7-configure-your-node-dev-settings)
8. [Run the Node](#8-run-the-node)

## 1. Install Go

---

Download and install the latest version of Go. As of February 2025, it's `1.24.0`.

[Go Installation Guide](https://go.dev/doc/install)

**Test Installation**:
Open a new terminal and execute:
```bash
go version
```
It should display `go1.24.0` or above.

---


## 2. Set Go Environment Variables

---


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

---


## 3. Python & Dependencies

---

Install python and the required packages.

### 3.1. Install Python (via Homebrew)


```bash
brew install python
```

By default, Homebrew will install Python 3.x and create symlinks like python3 and pip3. You can optionally create a symlink for python and pip if you want shorter commands (but check if they already exist first):

```bash
ln -s /opt/homebrew/bin/python3 /opt/homebrew/bin/python  # might already exist
ln -s /opt/homebrew/bin/pip3 /opt/homebrew/bin/pip        # might already exist
```



Below is an **updated** section for Python & dependencies that accounts for Homebrew’s newer **externally-managed environment** restrictions. Instead of installing PyYAML system-wide with `pip install`, the easiest approach is to use a **virtual environment** (or `pipx`, if you prefer).

You can simply **replace** the old steps in your guide with the version below:

---

## 3. Python & Dependencies

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
This should print out the installed PyYAML version (e.g., `6.0` or similar).

#### Alternative: Use `pipx` (for CLI tools)

If you need PyYAML as part of a **standalone command-line tool**, you could use [pipx](https://pypa.github.io/pipx/) instead:
```bash
brew install pipx
pipx install PyYAML
```
However, most Teranode workflows will need PyYAML as a library for scripts, so a virtual environment is usually best.


## 4. Project Dependencies

---

Install the various dependencies required for the project.

```bash
brew install golangci-lint
brew install staticcheck
brew install secp256k1
brew install protobuf
brew install protoc-gen-go
brew install protoc-gen-go-grpc
brew install libtool
brew install autoconf
brew install automake
```

**Test Protocol Buffers Installation**:
Execute:
```bash
protoc --version
```

---




## 5. Build & Install secp256k1

---


Clone, build, and install the `secp256k1` library:

```bash
git clone git@github.com:bitcoin-core/secp256k1.git
cd secp256k1
./autogen.sh
./configure
make
sudo make install
```

---


## 6. Clone the Project and Install Additional Dependencies

---

Clone the project:

```bash
git clone git@github.com:bitcoin-sv/teranode.git
```

**Install dependencies**:
Execute:
```bash
cd teranode

make install
```


> Note:
> If you receive an error `ModuleNotFoundError: No module named ‘yaml’` error, refer to this [issue](https://github.com/yaml/pyyaml/issues/291) for a potential fix. Example:
> ```bash
> PYTHONPATH=$HOME/Library/Python/3.9/lib/python/site-packages make install  #Make sure the path is correct for your own python version
> ```


---


Below is an **updated** example of how to add your own user settings to the `settings_local.conf`, given the new or changed lines in the file. The key idea is the same: **find** the existing template lines (those that end with `.NEW_USER_TEMPLATE`) and **duplicate** them, replacing `NEW_USER_TEMPLATE` with your name (or a unique identifier) in both the **key** and the **value**.

---

## 7. Configure Your Node Dev Settings (Updated Example)

### 7.1 Open and Inspect `settings_local.conf`

1. In your project directory, locate the file `settings_local.conf`.
2. Open it in your preferred editor (VSCode, IntelliJ, etc.).
3. **Search** for all lines containing `NEW_USER_TEMPLATE`. For example, you will see things like:

   ```conf
   clientName.dev.NEW_USER_TEMPLATE           = NEW_USER_TEMPLATE # template for future new users (referenced in documentation)
   coinbase_arbitrary_text.dev.NEW_USER_TEMPLATE = /NEW_USER_TEMPLATE/ # template
   asset_httpAddress.dev.NEW_USER_TEMPLATE       = http://bastion.ubsv.dev:18x90 # template for future new users
   ...
   ```

### 7.2 Duplicate and Customize the Template Lines

#### **Example:**

For example, if your name is **John**, you copy each line and change `NEW_USER_TEMPLATE` to `John`.

- **Original**:
  ```conf
  clientName.dev.NEW_USER_TEMPLATE           = NEW_USER_TEMPLATE
  coinbase_arbitrary_text.dev.NEW_USER_TEMPLATE = /NEW_USER_TEMPLATE/
  asset_httpAddress.dev.NEW_USER_TEMPLATE       = http://bastion.ubsv.dev:18x90
  ```

- **New lines** (added below the originals):
  ```conf
  clientName.dev.John           = John
  coinbase_arbitrary_text.dev.John = /John/
  asset_httpAddress.dev.John       = http://bastion.ubsv.dev:18x90
  ```

> If there’s already a `clientName.dev.John`, pick something more specific, like `JohnDoe`.

### 7.3 Set Your Environment Variable

In order for your node to read **your** custom lines, you set `SETTINGS_CONTEXT` to match the prefix you used (i.e., `dev.John`).

In **zsh**, open `~/.zprofile` (or `~/.zshrc`). In **bash**, open `~/.bash_profile` (or `~/.bashrc`).
Add:
```bash
export SETTINGS_CONTEXT=dev.John
```
*(Replace `John` with whatever identifier you used in your config.)*

After editing, **reload** your shell config:
```bash
source ~/.zprofile
```
(or the equivalent for your shell).

### 7.4 Verify

1. **Echo** the environment variable to ensure it’s set correctly:
   ```bash
   echo $SETTINGS_CONTEXT
   ```
   Should print `dev.John`.

2. **Run** or **restart** your node. Check logs or console output to confirm it’s picking up the lines with `dev.John`.

### 7.5 Commit Your Changes (Optional)

If this config is in a shared repository and your team expects each dev to commit their user-specific lines, go ahead and commit them:

```bash
git add settings_local.conf
git commit -m "Add dev.John config settings"
git push
```

----

## 8. Run the Node

As a precondition, we need to have Kafka (https://kafka.apache.org/) running in our local dev env:

```bash
./deploy/dev/kafka.sh
```


You can run the entire node with the following command:

```bash
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run .
```

If no errors are seen, you have successfully installed the project and are ready to start working on the project or running the node.

---

## 9. Troubleshooting

### 9.1. Dependency errors / conflics

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

- [Check our Git Commit Signing Setup Guide for Contributors](./gitCommitSigningSetupGuide.md)
