# How to Reset Teranode (Kubernetes)

Last modified: 29-October-2025

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Scale Down Services](#scale-down-services)
- [Database Cleanup](#database-cleanup)
    - [Aerospike Cleanup](#aerospike-cleanup)
    - [PostgreSQL Cleanup](#postgresql-cleanup)
- [Persistent Volume Cleanup](#persistent-volume-cleanup)
- [Restart Services](#restart-services)
- [Post-Reset Steps](#post-reset-steps)
    - [Verify Services Are Running](#verify-services-are-running)
    - [Set FSM State](#set-fsm-state)
    - [Monitor Synchronization](#monitor-synchronization)
    - [Consider Data Seeding](#consider-data-seeding)
- [Related Documentation](#related-documentation)

## Introduction

If you need to sync Teranode from scratch, restore from a backup, or clean up after testing, you'll need to reset the blockchain data. This guide provides reset procedures for Kubernetes deployments using the Teranode Operator.

**When to reset:**

- Starting a fresh synchronization from genesis
- Switching between networks (mainnet/testnet)
- Recovering from data corruption
- Cleaning up after testing or development
- Preparing for a data seeding operation

## Prerequisites

- **Backup important data** before proceeding (if applicable)
- **Stop all Teranode services** before reset
- **kubectl access** to the Kubernetes cluster
- **Appropriate RBAC permissions** for the teranode-operator namespace

> **⚠️ Warning:** These operations are **irreversible** and will delete all blockchain data. Ensure you have backups if needed.

## Scale Down Services

First, scale down the Teranode services to prevent data access during cleanup:

```bash
# Scale down by disabling the cluster in the CR
kubectl patch cluster teranode-cluster -n teranode-operator --type=merge -p '{"spec":{"enabled":false}}'

# Wait for pods to terminate
kubectl wait --for=delete pod -l app.kubernetes.io/managed-by=teranode-operator -n teranode-operator --timeout=300s

# Verify all Teranode pods are terminated
kubectl get pods -n teranode-operator
```

## Database Cleanup

### Aerospike Cleanup

```bash
# Get Aerospike pod name
AEROSPIKE_POD=$(kubectl get pods -n teranode-operator -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep aero)

# Access Aerospike pod
kubectl exec -it $AEROSPIKE_POD -n teranode-operator -- bash

# Truncate the UTXO set
asadm --enable -e "manage truncate ns utxo-store"

# Verify the total records count (should slowly decrease to 0)
asadm -e "info"

# Exit
exit
```

### PostgreSQL Cleanup

```bash
# Get Postgres pod name
POSTGRES_POD=$(kubectl get pods -n teranode-operator -l app=postgres -o jsonpath='{.items[0].metadata.name}')

# Access Postgres pod
kubectl exec -it $POSTGRES_POD -n teranode-operator -- psql -U postgres -d teranode

# Once in psql, run these commands:
```

```sql
-- Sanity count check
SELECT COUNT(*) FROM blocks;

-- Truncate tables
TRUNCATE TABLE blocks RESTART IDENTITY CASCADE;
TRUNCATE TABLE state RESTART IDENTITY CASCADE;
TRUNCATE TABLE bans RESTART IDENTITY CASCADE;

-- Verify truncation
SELECT COUNT(*) FROM blocks;

-- (Optional) Drop indexes for faster seeding
DROP INDEX IF EXISTS idx_chain_work_id;
DROP INDEX IF EXISTS idx_chain_work_peer_id;

-- Exit
\q
```

> **💡 Performance Tip:** Dropping the indexes before seeding significantly speeds up the initial data load. The blockchain service will recreate them automatically on startup.

## Persistent Volume Cleanup

If you need to clean up filesystem data from shared PVCs:

```bash
# List persistent volume claims
kubectl get pvc -n teranode-operator

# If you need to delete PVC data, you can:
# 1. Delete the PVC (this will delete all data on it)
kubectl delete pvc shared-pvc -n teranode-operator

# 2. Recreate the PVC (it will be recreated automatically by the operator)
# Or manually create it if needed

# Alternatively, access a pod with the PVC mounted and clean specific directories
kubectl exec -it <pod-with-pvc> -n teranode-operator -- rm -rf /data/teranode-operator/*
```

## Restart Services

Once cleanup is complete, scale services back up:

```bash
# Re-enable the cluster in the CR
kubectl patch cluster teranode-cluster -n teranode-operator --type=merge -p '{"spec":{"enabled":true}}'

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app=blockchain -n teranode-operator --timeout=300s

# Verify all pods are running
kubectl get pods -n teranode-operator
```

## Post-Reset Steps

After resetting Teranode, follow these steps to resume operations:

### Verify Services Are Running

```bash
kubectl get pods -n teranode-operator
```

### Set FSM State

Teranode starts in IDLE state. You need to transition to the appropriate state:

```bash
# Via teranode-cli
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate --fsmstate LEGACYSYNCING
```

### Monitor Synchronization

```bash
# View blockchain logs
kubectl logs -n teranode-operator -l app=blockchain -f

# Port-forward to access dashboard
kubectl port-forward -n teranode-operator service/asset 8090:8090
# Then open http://localhost:8090/viewer
```

### Consider Data Seeding

For faster synchronization, consider using data seeding instead of syncing from genesis. See the [How to Sync the Node](minersHowToSyncTheNode.md) guide for seeding procedures.

## Related Documentation

- [How to Sync the Node](minersHowToSyncTheNode.md) - Blockchain synchronization methods
- [Kubernetes Installation Guide](minersHowToInstallation.md) - Kubernetes deployment
- [How to Interact with FSM](../minersHowToInteractWithFSM.md) - FSM state management
