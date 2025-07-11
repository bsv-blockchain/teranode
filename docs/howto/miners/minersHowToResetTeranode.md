# How to Reset Teranode

If you require to sync a Teranode from scratch or need to restore from a backup, you will need to clean-up pre-existing data from your UTXO store, Blockchain store and your filesystem.

The below is an example procedure, assuming Aerospike as your UTXO Store and Postgres as your Blockchain store.

**1. Aerospike clean-up**

See the Aerospike documentation for more information.

```bash
asadm --enable -e "manage truncate ns utxo-store set utxo"

# verify the total records count, should slowly decrease to 0
asadm -e "info"
```

**2. Postgres clean-up**

```bash

# Connect to your the db used by Teranode.
postgres=> \c <db_name>

# Sanity count check.
teranode_mainnet=> SELECT COUNT(*) FROM blocks;
count
--------
123123
(1 row)

# truncate the blocks and state table.
DROP TABLE IF EXISTS blocks CASCADE;
DROP TABLE IF EXISTS state CASCADE;
DROP TABLE IF EXISTS bans CASCADE;
```

**3. Filesystem clean-up.**

```bash
DATA_MOUNT_POINT=/mnt/teranode # Correct the exact path as required
sudo rm -rf $DATA_MOUNT_POINT/*
```
