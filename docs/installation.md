# 🖥 Pre-requisites and Installation

This guide assists you in setting up the UBSV node project on your machine. The below assumes you are running a recent version of Mac OS.

## 1. Install Go

---

Download and install the latest version of Go. As of November 2023, it's `1.21.3`.

[Go Installation Guide](https://go.dev/doc/install)

**Test Installation**:
Open a new terminal and execute:
```bash
go version
```
It should display `go1.21.3`.

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


## 7. Run the Node

---

You can run the entire node with the following command:

```bash
SETTINGS_CONTEXT=dev go run .
```

If no errors are seen, you have successfully installed the project and are ready to start working on the project or running the node.

---
