# deploy/docker

Raw Docker Compose definitions used internally by Teranode releases.

> **Most operators should not start here.** The supported Docker path is the
> [teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
> repository. It wraps these compose files with setup, start, status, RPC,
> seeding, update, and cleanup scripts, and writes a `.env` you can edit.

## Use the quickstart

```bash
git clone https://github.com/bsv-blockchain/teranode-quickstart.git
cd teranode-quickstart
./setup.sh
./start.sh
```

See the operator guides:

- [Install Teranode with Docker](../../docs/howto/miners/docker/minersHowToInstallation.md)
- [Configure Docker Teranode](../../docs/howto/miners/docker/minersHowToConfigureTheNode.md)
- [Sync the Blockchain](../../docs/howto/miners/docker/minersHowToSyncTheNode.md)
- [Update Teranode](../../docs/howto/miners/docker/minersUpdatingTeranode.md)

## When to use this directory directly

You only need the files here for development against a checked-out Teranode
source tree, custom compose layouts, or environments where the quickstart
defaults do not fit. Familiarity with Docker Compose, the network-specific
overrides under `mainnet/`, `testnet/`, and `teratestnet/`, and the settings
in `base/` is required.

## Required shared service key

Before starting the stack, generate `grpc_admin_api_key` with
`openssl rand -hex 32` and supply it through the environment or a local `.env` file.
The base compose file passes the same key to all services. Blockchain refuses
startup without a valid key. Keep the file untracked and reuse the key for CLI
clients. See [authentication and upgrade order](../../docs/topics/services/blockchainAuthentication.md).
