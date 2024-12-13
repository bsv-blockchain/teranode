# Syncing the Blockchain

Teranode allows to sync the blockchain from a remote node, or from a local snapshot. The Teranode team publishes regular snapshots of the blockchain and UTXO set, which can be used to speed up the initial sync process.
For more information, please refer to the Teranode team.



## Handling Extended Downtime:

If Teranode has been offline for an extended period, consider the following:

1. **Database Integrity Check**:
- After a long downtime, it's advisable to check the integrity of your blockchain and UTXO databases.

- This can be done by examining the logs for any corruption warnings or errors.



2. **Manual Intervention**:
- In rare cases of significant divergence or data corruption, you might need to manually intervene.

- This could involve reseeding the node with a more recent blockchain snapshot or UTXO set.



3. **Monitoring Catch-up Time**:
- The time required for catch-up synchronization depends on the duration of the downtime and the number of missed blocks.
- Use monitoring tools (e.g., Prometheus and Grafana) to track the sync progress and estimate completion time, or follow the logs with `docker logs -f legacy -n 100`.
