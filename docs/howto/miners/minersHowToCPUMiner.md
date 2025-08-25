# How to Set Up CPU Mining with Teranode

Last modified: 02-Aug-2025

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [BSV CPU Miner Setup](#bsv-cpu-miner-setup)
- [Configuration Parameters](#configuration-parameters)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)

## Introduction

This guide provides instructions for setting up CPU mining with Teranode using the BSV CPU miner. CPU mining is primarily intended for testing purposes and small-scale mining operations.

**Important Note**: CPU mining is not recommended for production mining due to its low hash rate compared to ASIC miners. This setup is ideal for:

- Testing Teranode functionality
- Development environments
- Educational purposes
- Small-scale testnet mining

## Prerequisites

- Running Teranode instance with RPC service enabled
- Docker installed and configured
- Access to the Teranode network (Docker network or direct network access)
- Valid Bitcoin SV address for receiving mining rewards

## BSV CPU Miner Setup

The recommended CPU miner for Teranode is the BSV CPU miner available as a Docker container.

### Basic Configuration

```bash
docker run -it \
  --network my-teranode-network \
  ghcr.io/bitcoin-sv/cpuminer:latest \
  --algo=sha256d --debug --always-gmc --retries=1 --retry-pause=5 \
  --url=http://rpc:9292 --userpass=bitcoin:bitcoin \
  --coinbase-addr=mgqipciCS56nCYSjB1vTcDGskN82yxfo1G \
  --threads=2 --coinbase-sig="Teranode us-1"
```

### Configuration for External Network

If your Teranode RPC service is not on the same Docker network:

```bash
docker run -it \
  ghcr.io/bitcoin-sv/cpuminer:latest \
  --algo=sha256d --debug --always-gmc --retries=1 --retry-pause=5 \
  --url=http://YOUR_TERANODE_IP:9292 --userpass=bitcoin:bitcoin \
  --coinbase-addr=YOUR_BSV_ADDRESS \
  --threads=4 --coinbase-sig="Your Mining Pool"
```

## Configuration Parameters

### Required Parameters

| Parameter | Short Form | Description | Example |
|-----------|------------|-------------|---------|
| `--algo` | `-a` | Mining algorithm (always sha256d for BSV) | `--algo=sha256d` |
| `--url` | `-o` | RPC server URL | `--url=http://rpc:9292` |
| `--userpass` | `-O` | RPC credentials (username:password) | `--userpass=bitcoin:bitcoin` |
| `--coinbase-addr` | | Address to receive mining rewards | `--coinbase-addr=1ABC...` |

### Optional Parameters

| Parameter | Short Form | Description | Default | Example |
|-----------|------------|-------------|---------|---------|
| `--threads` | `-t` | Number of mining threads | CPU cores | `--threads=4` |
| `--coinbase-sig` | | Custom signature for mined blocks | Empty | `--coinbase-sig="My Pool"` |
| `--retries` | `-r` | Number of retries for failed requests | 3 | `--retries=10` |
| `--debug` | `-D` | Enable debug output | false | `--debug` |
| `--always-gmc` | | Always use getminingcandidate RPC | false | `--always-gmc` |

### Advanced Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--scantime` | Time to spend on each work unit (seconds) | `--scantime=30` |
| `--timeout` | Timeout for RPC requests (seconds) | `--timeout=60` |
| `--retry-pause` | Pause between retries (seconds) | `--retry-pause=5` |

## Usage Examples

### Testnet Mining

```bash
docker run -it \
  --network teranode-testnet \
  ghcr.io/bitcoin-sv/cpuminer:latest \
  --algo=sha256d --always-gmc --retries=1 --retry-pause=5 \
  --url=http://rpc:9292 \
  --userpass=bitcoin:bitcoin \
  --coinbase-addr=mgqipciCS56nCYSjB1vTcDGskN82yxfo1G \
  --threads=2 \
  --coinbase-sig="Testnet Miner" \
  --debug
```

### High-Performance Configuration

For systems with many CPU cores:

```bash
docker run -it \
  --network my-teranode-network \
  ghcr.io/bitcoin-sv/cpuminer:latest \
  --algo=sha256d --always-gmc --retries=1 --retry-pause=5 \
  --url=http://rpc:9292 \
  --userpass=bitcoin:bitcoin \
  --coinbase-addr=YOUR_BSV_ADDRESS \
  --threads=16 \
  --scantime=60 \
  --coinbase-sig="High Performance Miner"
```

### Quiet Mode (No Debug Output)

```bash
docker run -it \
  --network my-teranode-network \
  ghcr.io/bitcoin-sv/cpuminer:latest \
  --algo=sha256d --always-gmc --retries=1 --retry-pause=5 \
  --url=http://rpc:9292 \
  --userpass=bitcoin:bitcoin \
  --coinbase-addr=YOUR_BSV_ADDRESS \
  --threads=4 \
  --coinbase-sig="Production Miner"
```

## Troubleshooting

### Common Issues

**Issue**: Connection refused errors

```text
[ERROR] HTTP request failed: Connection refused
```

**Solution**:

- Verify Teranode RPC service is running: `docker ps | grep rpc`
- Check network connectivity between miner and RPC service
- Ensure correct URL format: `http://hostname:port`

**Issue**: Authentication failures

```text
[ERROR] JSON-RPC call failed: Authentication failed
```

**Solution**:

- Verify RPC credentials in Teranode configuration
- Default credentials are `bitcoin:bitcoin`
- Check userpass format: `username:password`

**Issue**: Invalid address errors

```text
[ERROR] Invalid coinbase address
```

**Solution**:

- Use a valid BSV address for your network (mainnet/testnet)
- Testnet addresses start with 'm' or 'n'
- Mainnet addresses start with '1' or '3'

**Issue**: Low hash rate or no shares

```text
[INFO] No shares submitted
```

**Solution**:

- Increase thread count if system can handle it
- Verify Teranode is fully synchronized
- Check network difficulty - CPU mining may take time to find shares

### Performance Optimization

1. **Thread Count**: Start with CPU core count, adjust based on system performance
2. **Scan Time**: Increase for better efficiency, decrease for faster response to new work
3. **System Resources**: Ensure adequate cooling and power for sustained mining

### Monitoring

Monitor your mining progress:

```bash
# View miner logs
docker logs <container_id>

# Check Teranode RPC status
curl --user bitcoin:bitcoin \
  --data-binary '{"jsonrpc":"1.0","id":"test","method":"getmininginfo","params":[]}' \
  -H 'content-type: text/plain;' \
  http://localhost:9292/
```

### Getting Mining Statistics

Check current mining information:

```bash
# Get network hash rate and difficulty
curl --user bitcoin:bitcoin \
  --data-binary '{"jsonrpc":"1.0","id":"test","method":"getnetworkhashps","params":[]}' \
  -H 'content-type: text/plain;' \
  http://localhost:9292/

# Check latest blocks
curl --user bitcoin:bitcoin \
  --data-binary '{"jsonrpc":"1.0","id":"test","method":"getbestblockhash","params":[]}' \
  -H 'content-type: text/plain;' \
  http://localhost:9292/
```

## Security Considerations

1. **RPC Access**: Ensure RPC service is not exposed to untrusted networks
2. **Credentials**: Use strong RPC credentials in production environments
3. **Network Isolation**: Run miner in isolated network environment when possible
4. **Address Security**: Keep private keys for mining addresses secure

## Additional Resources

- [Interacting with RPC Service](./minersHowToInteractWithRPCServer.md)
- [Teranode CLI Guide](./minersHowToTeranodeCLI.md)
- [Docker Installation Guide](./docker/minersHowToInstallation.md)
- [BSV CPU Miner GitHub Repository](https://github.com/bitcoin-sv/cpuminer)

---

> **Note**: CPU mining is primarily for testing and development. For production mining operations, consider using ASIC miners or other specialized mining hardware.
