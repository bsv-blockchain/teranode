# How to Backup Teranode Data

Last modified: 6-March-2025


Regular and secure backups are essential for protecting a Teranode installation, ensuring data integrity, and safeguarding your wallet. The next steps outline how to back up your Teranode wallet and data:

There are three main options for backing up and restoring a Teranode system. Each option has its own use cases and procedures.

## Option 1: Full System Backup

This option involves stopping all services, then performing backups of both the UTXO data and the blockchain data, plus the node data (typically under the /data directory).

In a production environment, UTXOs will be typically stored in Aerospike, and the blockchain data will be stored in Postgres.

The below is an example procedure, however you must adapt this to your specific needs.

### Procedure:

Note: This is not a hot backup. All services must be stopped before performing the backup.

1. Stop all Teranode services.
2. Perform an UTXO data backup.
3. Perform a blockchain data backup.
4. Perform a backup of the filesystem data (typically under /data).
4. Restart services after backup completion.


## Option 2: Archive Mode Backup

If running the node in Archive mode (with Block Persister and UTXO Persister), users can use their own exported UTXO set as an alternative to exporting the Aerospike data.

### Procedure:

1. Ensure Block Persister and UTXO Persister are enabled and running.
2. Use the persisted UTXO set and header files, together with a blockchain postgres export, for backup purposes.
3. Follow the restoration procedure in the Installation section.
