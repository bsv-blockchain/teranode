# How to Start and Stop Teranode in Kubernetes

Last modified: 6-March-2025

## Introduction

This guide provides instructions for starting and stopping Teranode in a Kubernetes environment using the Teranode Operator and associated configurations.

## Prerequisites

Before proceeding, ensure you have all components installed as described in the [Installation Guide](minersHowToInstallation.md):

- Docker
- Minikube
- kubectl
- Helm
- AWS CLI
- All dependencies deployed (Aerospike, PostgreSQL, Kafka)
- Storage provider configured (NFS)

## Starting Teranode

### 1. Deploy Teranode Configuration

```bash
# Apply the Teranode configuration and custom resources
kubectl apply -f kubernetes/teranode/teranode-configmap.yaml -n teranode-operator
kubectl apply -f kubernetes/teranode/teranode-cr.yaml -n teranode-operator
```

### 2. Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n teranode-operator | grep -E 'aerospike|postgres|kafka|teranode-operator'

# Check Teranode services are ready
kubectl wait --for=condition=ready pod -l app=blockchain -n teranode-operator --timeout=300s

# View Teranode logs
kubectl logs -n teranode-operator -l app=blockchain -f
```

### 3. Start Syncing (if needed)

```bash
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate -fsmstate running
```

## Stopping Teranode

### 1. Graceful Shutdown

```bash
# Remove the Teranode custom resource
kubectl delete -f kubernetes/teranode/teranode-cr.yaml -n teranode-operator

# Remove the configmap
kubectl delete -f kubernetes/teranode/teranode-configmap.yaml -n teranode-operator
```

### 2. Verify Resource Removal

```bash
# Check pod termination status
kubectl get pods -n teranode-operator

# Monitor shutdown events
kubectl get events -n teranode-operator
```

### 3. Force Deletion (if necessary)

Only use these commands if normal shutdown fails:

```bash
# Force delete resources
kubectl delete -f kubernetes/teranode/teranode-cr.yaml -n teranode-operator --grace-period=0 --force
kubectl delete -f kubernetes/teranode/teranode-configmap.yaml -n teranode-operator --grace-period=0 --force
```

## Other Resources

- [How to Install Teranode](minersHowToInstallation.md)
- [Third Party Reference Documentation](../../../references/thirdPartySoftwareRequirements.md)
- [Teranode Sync Guide](minersHowToSyncTheNode.md)
