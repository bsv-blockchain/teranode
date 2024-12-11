# Syncing the Blockchain


## Index

- [Introduction](#introduction)
- [Synchronization Process](#synchronization-process)


## Introduction

When a new Teranode instance is deployed in a Kubernetes cluster, it begins the synchronization process automatically. This process, known as the Initial Block Download (IBD), involves downloading and validating the entire blockchain history from other nodes in the network.


## Synchronization Process

1. **Peer Discovery**:
- Upon startup, the Teranode peer service (typically running in the `peer` pod) begins to discover and connect to other nodes in the BSV network.



2. **Block Download**:
- Once connected, Teranode requests blocks from its peers, typically starting with the first node it successfully connects to.

- This peer could be either a traditional BSV node or another BSV Teranode.



3. **Validation and Storage**:
- As blocks are received, they are validated by the various Teranode services (e.g., `block-validator`, `subtree-validator`).

- Valid blocks are then stored in the blockchain database, managed by the `blockchain` service.

- The UTXO (Unspent Transaction Output) set is updated accordingly, typically managed by the `asset` service.



4. **Progress Monitoring**:
- You can monitor the synchronization progress by checking the logs of relevant pods:
```
kubectl logs <blockchain-pod-name>
kubectl logs <peer-pod-name>
```
- The `getblockchaininfo` command can provide detailed sync status:
```
kubectl exec <blockchain-pod-name> -- /app/ubsv.run -blockchain=1 getblockchaininfo
```



##### Optimizing Initial Sync


###### Seeding Teranode from a bitcoind instance

To speed up the initial synchronization process, you have the option to seed Teranode from a bitcoind instance.

Precondition: You must have a bitcoind instance running.

1. Stop your bitcoind instance.
2. Export the level DB from the bitcoind instance, and mount it in a location accessible to Teranode.
3. Run the Teranode legacy seeder:

**TODO** <-----------

4. Start Teranode.


###### Seeding Teranode with pre-existing Teranode data

To speed up the initial synchronization process, you have the option to seed Teranode with pre-existing Teranode data:

1. **Pre-created UTXO Set**:

- Prepare a PersistentVolume with a pre-created UTXO set.

- Configure your Cluster resource to use this PersistentVolume for the `asset` service.



2. **Existing Blockchain DB**:
- Similarly, prepare a PersistentVolume with an existing blockchain database.

- Configure your Cluster resource to use this PersistentVolume for the `blockchain` service.


**TODO** <-----------

3. **Applying Pre-existing Data**:
- Update your Cluster custom resource to point to these pre-populated PersistentVolumes.
- Apply the updated Cluster resource:
```
kubectl apply -f teranode_your_cluster.yaml
```

By using pre-existing data, Teranode can skip a significant portion of the initial sync process, starting from a more recent point in the blockchain history.



##### Recovery After Downtime

In a Kubernetes environment, Teranode is designed to be resilient and can recover from various types of downtime or disconnections.



1. **Automatic Restart**:
- If a pod crashes or is terminated, Kubernetes will automatically restart it based on the deployment configuration.

- This is handled by the ReplicaSet controller in Kubernetes.



2. **Reconnection**:
- Upon restart, the `peer` service will re-establish connections with other nodes in the network.



3. **Block Request**:
- Teranode will determine the last block it has and request subsequent blocks from its peers.

- This process is automatic and doesn't require manual intervention.



4. **Catch-up Synchronization**:
- The node will download and process all blocks it missed during the downtime.

- This process is typically faster than the initial sync as it involves fewer blocks.



5. **Monitoring Recovery**:

- Monitor the recovery process using the same methods as the initial sync:
```
kubectl logs <peer-pod-name>
kubectl exec <blockchain-pod-name> -- /app/ubsv.run -blockchain=1 getblockchaininfo
```



##### Handling Extended Downtime:

If Teranode has been offline for an extended period, consider the following:

1. **Database Integrity Check**:
- After a long downtime, it's advisable to check the integrity of your blockchain and UTXO databases.

- This can be done by examining the logs for any corruption warnings or errors.



2. **Manual Intervention**:
- In rare cases of significant divergence or data corruption, you might need to manually intervene.

- This could involve reseeding the node with a more recent blockchain snapshot or UTXO set.



3. **Monitoring Catch-up Time**:
- The time required for catch-up synchronization depends on the duration of the downtime and the number of missed blocks.
- Use monitoring tools (e.g., Prometheus and Grafana) to track the sync progress and estimate completion time.
