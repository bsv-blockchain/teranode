# How to Backup Teranode Data

Regular and secure backups are essential for protecting a Teranode installation, ensuring data integrity, and safeguarding your wallet. The next steps outline how to back up your Teranode wallet and data:

There are three main options for backing up and restoring a Teranode system. Each option has its own use cases and procedures.

#### Option 1: Full System Backup

This option involves stopping all services, then performing backups of both the UTXO data and the blockchain data.

In a production environment, UTXOs will be typically stored in Aerospike, and the blockchain data will be stored in Postgres.

The below is an example procedure, however you must adapt this to your specific needs.

##### Procedure:

1. Stop all Teranode services.
2. Perform an Aerospike UTXO backup.
3. Perform a PostgreSQL blockchain backup.
4. Restart services after backup completion.

##### Backup Commands:

###### Aerospike Backup (Version 6.3.12):
```bash
REGION=eu-west-1
asbackup --s3-region $REGION \
-d s3://$PATH \
--s3-endpoint-override https://s3.$REGION.amazonaws.com \
-n utxo-store --parallel 64 -r -z zstd
```

###### PostgreSQL Backup (Version 16):
```bash
./pg_dump -Fc -Z 9 -d $DB-1 -U $USERNAME -f ./$DB-1.pg_dump -h postgres-postgresql.postgres
```

##### Restore Commands:

###### PostgreSQL Restore:
```bash
pg_restore -d $DB-1 -U $USERNAME -h localhost ./$DB-1.pg_dump
```

###### Aerospike Restore:
```bash
REGION=eu-west-1
asrestore --ignore-record-error --s3-region $REGION \
-d s3://$PATH \
--s3-endpoint-override https://s3.$REGION.amazonaws.com \
-n utxo-store --parallel 64 --compress zstd
```

Note: This is not a hot backup. All services must be stopped before performing the backup.

#### Option 2: Archive Mode Backup

If running the node in Archive mode (with Block Persister and UTXO Persister), users can use their own exported UTXO set as an alternative to exporting the Aerospike data.

##### Procedure:

1. Ensure Block Persister and UTXO Persister are enabled and running.
2. Use the persisted UTXO set files, together with a blockchain postgres export, for backup purposes.
3. Follow the restoration procedure in the Installation section.

#### Option 3: Fresh UTXO and Blockchain Export

Operators can download a fresh UTXO and blockchain export from the Teranode repository to reset the node to a specific point.

##### Procedure:

1. Download the latest UTXO and blockchain export from the Teranode repository.
2. Stop all Teranode services.
3. Replace existing UTXO and blockchain data with the downloaded exports.
4. Restart Teranode services.
5. The node will automatically sync up to the current blockchain height.
