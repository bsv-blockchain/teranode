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
