# Configuring Kubernetes Operator Teranode

Last modified: 13-December-2024

## Index

- [Configuring Setting Files](#configuring-setting-files)
- [Optional vs Required services](#optional-vs-required-services)
- [Reference Settings](#reference---settings)

## Configuring Setting Files

The following settings can be configured in the custom operator ConfigMap. You can refer to the notes on the installation and ConfigMap setup [here](kubernetes/minersHowToInstallation.md).

For a list of settings, and their default values, please refer to the reference at the end of this document.


## Optional vs Required services

While most services are required for the proper functioning of Teranode, some services are optional and can be disabled if not needed. The following table provides an overview of the services and their status:

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

The Block and UTXO persister services are optional and can be disabled. If enabled, your node will be in Archive Mode, storing historical block and UTXO data.
As Teranode does not retain historical transaction data, this data can be useful for analytics and historical lookups, but comes with additional storage and processing overhead.
Additionally, it can be used as a backup for the UTXO store.

## Reference - Settings

You can find the pre-configured settings file [here](https://github.com/bitcoin-sv/teranode-public/blob/master/docker/base/settings_local.conf). These are the pre-configured settings in your docker compose. You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.
