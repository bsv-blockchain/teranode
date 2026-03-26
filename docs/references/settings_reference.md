# Comprehensive Settings Reference

This document provides a complete reference for all Teranode configuration settings, organized by component.

## Table of Contents

1. [Overview](#overview)
2. [General Configuration](#general-configuration)
3. [Services](#services)

## Overview

All Teranode services accept settings through a centralized Settings object that allows local and remote servers to have their own specific configuration.

For general information on how the configuration system works, see the [Settings Overview](settings.md).

For deployment-specific information, see:

- [Developer Setup](../tutorials/developers/developerSetup.md)
- [Docker Configuration](../howto/miners/docker/minersHowToConfigureTheNode.md)
- [Kubernetes Configuration](../howto/miners/kubernetes/minersHowToConfigureTheNode.md)

---

## General Configuration

### Configuration Files

Settings are stored in two files:

- `settings.conf`: Global settings with sensible defaults for all environments
- `settings_local.conf`: Developer-specific and deployment-specific overrides (not in source control)

### Configuration System

The configuration system uses a layered approach with the following priority (highest to lowest):

1. Environment variable (exact setting key name, e.g., `asset_httpListenAddress=:8090`)
2. `SETTING_NAME.full_context` (e.g., `SETTING_NAME.docker.m` — longest matching context chain wins)
3. Progressively shorter context suffixes (e.g., `SETTING_NAME.docker` if `SETTING_NAME.docker.m` is not set)
4. `SETTING_NAME`: Base setting (lowest priority)

Context resolution strips suffixes from right to left until a match is found. There is no special `.base` suffix — the plain key name is the final fallback.

### Context Names and Suffixes

Base contexts:

| Context | Description |
|---------|-------------|
| `dev` | Local development |
| `test` | Unit/integration test runs |
| `docker` | Docker Compose deployments |
| `operator` | Kubernetes/production deployments |

Contexts support dot-notation suffixes to target specific deployment sub-configurations:

| Suffix | Meaning | Example |
|--------|---------|---------|
| `.m` | Multi-node Docker Compose setup | `KAFKA_HOSTS.docker.m` |
| `.ss.teranode1` | Single-service deployment, first instance | `KAFKA_BLOCKS.docker.ss.teranode1` |
| `.testrunner` | CI test runner environment | `DATADIR.docker.context.testrunner` |

Suffixes are combined with base contexts using dots, e.g., `docker.m` means "multi-node Docker Compose". Settings are resolved by matching the longest applicable suffix chain.

### Environment Variables

Settings can be configured as environment variables using the exact setting key name (e.g., `asset_httpListenAddress=:8090`, `blockchain_grpcAddress=localhost:8087`). The environment variable name matches the key exactly as it appears in `settings.conf` and the settings structs. Environment variables take the highest priority and override any value in the settings files.

---

## Services

For detailed service-specific configuration documentation, see:

- **[Alert Service](settings/services/alert_settings.md)** - Bitcoin SV alert system configuration
- **[Asset Server](settings/services/asset_settings.md)** - HTTP/WebSocket interface configuration
- **[Block Assembly](settings/services/blockassembly_settings.md)** - Block assembly service configuration
- **[Blockchain](settings/services/blockchain_settings.md)** - Blockchain state management configuration
- **[Block Persister](settings/services/blockpersister_settings.md)** - Block persistence configuration
- **[Block Validation](settings/services/blockvalidation_settings.md)** - Block validation configuration
- **[Legacy](settings/services/legacy_settings.md)** - Legacy Bitcoin protocol compatibility configuration
- **[P2P](settings/services/p2p_settings.md)** - Peer-to-peer networking configuration
- **[Propagation](settings/services/propagation_settings.md)** - Transaction propagation configuration
- **[RPC](settings/services/rpc_settings.md)** - JSON-RPC server configuration
- **[Pruner](settings/services/pruner_settings.md)** - UTXO and block data pruning configuration
- **[Subtree Validation](settings/services/subtreevalidation_settings.md)** - Subtree validation configuration
- **[UTXO Persister](settings/services/utxopersister_settings.md)** - UTXO set persistence configuration
- **[Validator](settings/services/validator_settings.md)** - Transaction validation configuration
- **[Coinbase](settings/services/coinbase_settings.md)** - Coinbase transaction and mining reward configuration
- **[Faucet](settings/services/faucet_settings.md)** - Test Bitcoin faucet service configuration
