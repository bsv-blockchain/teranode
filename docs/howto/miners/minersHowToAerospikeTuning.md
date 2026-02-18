# Aerospike - Configuration Considerations

Last Modified - 13-December-2024

# General notes

Teranode is tested at scaled-out configuration (1m tps throughput) with Aerospike. The Aerospike database is used to store the UTXO set, where indexes are stored in-memory, and data on disk.This document outlines some configuration notes for Aerospike.

If using the `Docker compose` installation path, your compose file will automatically install the Aerospike 7.1 community edition for you.

Expiration settings:

* For mainnet, it's recommended to enable expiration of spent UTXO records. This will reduce the memory and disk usage. You can control the expiration with the &expiration=${seconds} query parameter of the utxostore setting.

* For testnet, it's recommended to not use expiration because of an issue with 0-satoshis outputs being spent. This will be fixed in a future release.

The default configuration is set stop writing when 50% of the system memory has been consumed. To future-proof, consider running a dedicated cluster, with more memory and disk space allocated.

* See the [sample aerospike.conf](https://github.com/bsv-blockchain/teranode/blob/main/deploy/docker/base/aerospike.conf) for an example of configuration of the `stop-writes-sys-memory-pct` parameter.

By default, the Aerospike data is written to a single mount mounted in the aerospike container. For performance reasons, it is recommended to use at least 4 dedicated disks for the Aerospike data.

* See the [sample aerospike.conf](https://github.com/bsv-blockchain/teranode/blob/main/deploy/docker/base/aerospike.conf) for an example of configuration of the `storage-engine` parameter.

# Sanity Checking

```bash
asadm -e "info"
asadm -e "summary -l"
```

For more information about Aerospike, you can access the [Aerospike documentation](https://www.aerospike.com/docs).
