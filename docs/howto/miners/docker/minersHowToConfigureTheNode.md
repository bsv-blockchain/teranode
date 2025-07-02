# Configuring Docker Compose Teranode

Last modified: 22-January-2025

## Index

- [Configuring Setting Files](#configuring-setting-files)
- [Optional vs Required services](#optional-vs-required-services)
- [Reference Settings](#settings-reference)

## Configuring Setting Files


The Docker Compose installation provides a pre-configured set of settings to make the installation fully operational.

However, you can change the configuration and override any of the default settings in your local `settings_local.conf` file. To check how this file is created, please review the Docker installation guide [here](./minersHowToInstallation.md).

For a list of settings, and their default values, please refer to the reference at the end of this document.

## Environment Variables

As an alternative to configuring settings in `settings_local.conf`, you can also overwrite any setting using environment variable. Please see the `x-teranode-settings` in your `docker-compose.yml` for an example of how to proceed.

## Optional vs Required services

While most services are required for the proper functioning of Teranode, some services are optional and are disabled in Docker Compose. The following table provides an overview of the services and their status:

| Required          | Optional          |
|-------------------|-------------------|
| Asset Server      | Block Persister   |
| Block Assembly    | UTXO Persister    |
| Block Validator   |                   |
| Subtree Validator |                   |
| Blockchain        |                   |
| Propagation       |                   |
| P2P               |                   |
| Legacy Gateway    |                   |

The Block and UTXO persister services are optional and can be disabled. If enabled, your node will be in Archive Mode, storing historical block and UTXO data. This data can be useful for analytics and historical lookups but comes with additional storage and processing overhead. Additionally, it can be used as a backup for the UTXO store.


## Settings Reference

You can find the pre-configured settings file [here](https://github.com/bitcoin-sv/teranode/blob/main/settings.conf). You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.
