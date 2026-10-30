# How to Back Up and Restore Teranode Data

Last modified: 19-October-2026

## Scope

Teranode does not ship a backup or restore tool. Backup and restore of the
backing datastores is the responsibility of the operator or hosting provider.
This includes Aerospike, PostgreSQL, Kafka and the blob volumes. Use the
tools and procedures of your datastore vendor and your platform:

- Aerospike: [Aerospike backup and restore](https://aerospike.com/docs/tools/backup)
- PostgreSQL: [PostgreSQL backup and restore](https://www.postgresql.org/docs/current/backup.html)
- Kubernetes volumes: the volume snapshot support of your storage class
- Docker volumes: your host or volume backup practice

You must test your restore procedure on your own infrastructure. A backup that
nobody has restored from does not prove that recovery works.

This page tells you what Teranode needs from a backup, and the recovery paths
that Teranode itself provides.

## What to Capture

A Teranode node state is made of these parts. Capture all of them together:

| Part | Typical location |
|------|------------------|
| UTXO store | Aerospike (production), or the `utxostore` URL |
| Blockchain store | PostgreSQL, or the `blockchain_store` URL |
| Blob stores: blocks, subtrees, transactions and persisted files | `/data`, or the volumes and buckets that the store settings point to |
| Configuration | `.env` and `settings_local.conf` (Docker), or the ConfigMap and the `teranode-operator-secrets` Secret (Kubernetes) |

The UTXO store, the blockchain store and the blob stores must come from the
same point in time. A set that mixes points in time is not a valid node state.
Block validation can then reject blocks that the rest of the network accepts.

Teranode does not support a hot backup. Stop all Teranode services before you
take the backup, and keep them stopped until every part is captured.

## Option 1: Full System Backup

### Back Up

1. Stop all Teranode services:
    - Docker: see [Start and Stop Docker Teranode](docker/minersHowToStopStartDockerTeranode.md).
    - Kubernetes: see [Start and Stop Teranode in Kubernetes](kubernetes/minersHowToStopStartKubernetesTeranode.md).
2. Back up the UTXO store.
3. Back up the blockchain store.
4. Back up the blob stores.
5. Back up the configuration.
6. Start the services again.

### Restore

1. Stop all Teranode services.
2. Restore the UTXO store, the blockchain store and the blob stores from the
   same backup set.
3. Make sure that the configuration matches the restored data. The network and
   the Teranode version must be the same as when you took the backup. Do not
   restore data from one network into a configuration for another network.
4. Start the services. On Kubernetes, also recreate the Secret first if you
   removed it.
5. Check the FSM state with `teranode-cli getfsmstate`. If it reads `IDLE`,
   start catch-up with `teranode-cli setfsmstate --fsmstate catchingblocks`.
6. Validate the restored state against the chain tip:
    - Compare the restored best block (`getblockchaininfo` over
      [RPC](minersHowToInteractWithRPCServer.md)) with the block hash at the
      same height on a public block explorer.
    - Watch the node catch up to the network tip, and check that block
      validation accepts the new blocks.
    - If block validation rejects a block that the network accepted, the
      backup set is not consistent. Restore a different backup set, or reset
      and seed the node (see Option 2).

The Docker sync guide also covers this case in
[Restore Existing Data](docker/minersHowToSyncTheNode.md#restore-existing-data).

## Option 2: Seed from a UTXO Set

This option uses the recovery path that Teranode provides. You do not restore
the Aerospike data. Instead, you load a UTXO set and its headers into an empty
node with the seeder.

The UTXO set can come from these sources:

- The UTXO Persister and the Block Persister of your own node, if you run
  them.
- A compatible published snapshot.
- An export from an SV Node.

### Procedure

1. If you use your own node as the source, make sure that the Block Persister
   and the UTXO Persister are enabled and running. Keep a copy of the
   persisted UTXO set and header files for a block that you choose.
2. Reset the node that you want to recover:
    - Docker: [Reset Docker Teranode](docker/minersHowToResetTeranode.md)
    - Kubernetes: [Reset Teranode (Kubernetes)](kubernetes/minersHowToResetTeranode.md)
3. Seed the node and start catch-up:
    - Docker: [Sync Docker Teranode](docker/minersHowToSyncTheNode.md#seed-from-a-snapshot)
    - Kubernetes: [Seeding from Existing Teranode](kubernetes/minersHowToSyncTheNode.md#method-3-seeding-from-existing-teranode)
4. Verify the seeded tip before you start catch-up. See
   [Verify a Seed Before Catch-up](docker/minersHowToSyncTheNode.md#verify-a-seed-before-catch-up).

The seeder does not check the UTXO set against the chain, and it does not check
the headers file and the UTXO set against each other. Seed only from files that
you exported yourself or from an operator that you trust. See the trust
boundary in [Why Seeding Instead of Full IBD](docker/minersHowToSyncTheNode.md#why-seeding-instead-of-full-ibd).

## Backups and Upgrades

Teranode applies blockchain store schema changes on startup, and it only
applies them forward. An older version does not remove a change that a newer
version made. Before you upgrade across a release that changes data formats,
take a full system backup. To roll back across such a release, restore that
backup. Do not only change the image version.

See [Update Docker Teranode](docker/minersUpdatingTeranode.md) and
[Update Teranode with Kubernetes Helm](kubernetes/minersUpdatingTeranode.md).
