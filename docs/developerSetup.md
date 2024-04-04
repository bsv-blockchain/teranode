# 🖥 Developer Setup - Pre-requisites and Installation

This guide assists you in setting up the UBSV node project on your machine. The below assumes you are running a recent version of Mac OS.


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

Download and install the latest version of Go. As of January 2024, it's `1.21.6`.

[Go Installation Guide](https://go.dev/doc/install)

**Test Installation**:
Open a new terminal and execute:
```bash
go version
```
It should display `go1.21.6`.

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

### Python:


```bash
brew install python
ln -s /opt/homebrew/bin/python3 /opt/homebrew/bin/python  #might already exist
```

### Pip (Python Package Installer):

```bash
ln -s /opt/homebrew/bin/pip3 /opt/homebrew/bin/pip #might already exist
```

### PyYAML:

```bash
pip install pyyaml
```

---

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
git clone git@github.com:bitcoin-sv/ubsv.git
```

**Install dependencies**:
Execute:
```bash
cd ubsv

make install
```


> Note:
> If you receive an error `ModuleNotFoundError: No module named ‘yaml’` error, refer to this [issue](https://github.com/yaml/pyyaml/issues/291) for a potential fix. Example:
> ```bash
> PYTHONPATH=$HOME/Library/Python/3.9/lib/python/site-packages make install  #Make sure the path is correct for your own python version
> ```


---


## 7. Configure Your Node Dev Settings

Follow these steps to add the required settings to the `settings_local.conf` file when first setting up your computer:

1. **Open the `settings_local.conf` file** in your text editor (e.g., Visual Studio Code, Goland, Atom, Sublime, etc.).

2. **Locate the template settings**. They will look like:

   ```conf
   coinbase_arbitrary_text.dev.NEW_USER_TEMPLATE=/NEW_USER_TEMPLATE/ # template for future new users (referenced in documentation)
   asset_httpAddress.dev.NEW_USER_TEMPLATE=https://bastion.ubsv.dev:18x90 # template for future new users (referenced in documentation)
   asset_clientName.dev.NEW_USER_TEMPLATE=NEW_USER_TEMPLATE # template for future new users (referenced in documentation)
   ```

3. **Duplicate each line** and replace `NEW_USER_TEMPLATE` with your first name. If there's already another user with the same first name, use both your first name and last name.

   **Example**:
   If your name is `John Doe`, and there's no existing user named `John`, your modified settings would look like:

   ```conf
   settings_local.conf:coinbase_arbitrary_text.dev.John=/John/
   settings_local.conf:asset_httpAddress.dev.John=https://bastion.ubsv.dev:18x90
   settings_local.conf:asset_clientName.dev.John=John
   ```

   If there's already a user named `John`, then use:

   ```conf
   settings_local.conf:coinbase_arbitrary_text.dev.JohnDoe=/JohnDoe/
   settings_local.conf:asset_httpAddress.dev.JohnDoe=https://bastion.ubsv.dev:18x90
   settings_local.conf:asset_clientName.dev.JohnDoe=JohnDoe
   ```

4. **Save the file**.

5. **Commit the changes to the repository**:

   ```bash
   git commit -m "Added required Dev settings for [Your Name]"
   git push
   ```

   Replace `[Your Name]` with your actual name in the commit message.


6. **Add an entry to your .zprofile / .bashprofile to export your settings identifier**:


```bash
export SETTINGS_CONTEXT=dev.[NEW_USER_TEMPLATE] # Example for John Doe: export SETTINGS_CONTEXT=dev.JohnDoe
```

----

## 8. Run the Node

---

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
