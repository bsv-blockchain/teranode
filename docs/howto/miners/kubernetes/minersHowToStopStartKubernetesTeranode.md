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

#### Grace period must exceed the service stop timeout

On shutdown, each Teranode service is stopped under a bounded per-service
timeout, `service_manager_stopTimeout` (default `30s`). Within that window a
service flushes its Kafka producers and drains its in-memory batchers before its
transport closes; cutting it short drops queued work (e.g. tx-meta and
notifications).

The orchestrator's termination grace period must therefore be **strictly greater
than** the configured `service_manager_stopTimeout`, otherwise the orchestrator
sends `SIGKILL` mid-flush and defeats the bounded drain:

- **Kubernetes:** Kubernetes defaults `terminationGracePeriodSeconds` to **30
  seconds**, which exactly equals Teranode's default `service_manager_stopTimeout`
  of 30s and therefore leaves **no** shutdown margin — a service that uses its
  full stop budget can be `SIGKILL`ed before it finishes flushing. Set the pod
  `terminationGracePeriodSeconds` above `service_manager_stopTimeout` (recommended
  **≥ 45s** for the default 30s timeout, to leave margin for the kubelet's own
  SIGTERM→SIGKILL accounting).
- **Docker Compose:** set `stop_grace_period` above the timeout. The base
  Compose definition (`deploy/docker/base/docker-teranode.yml`) ships `60s` for
  every Teranode app service; Compose's unset default is only `10s`, which is
  below the 30s budget.

If you raise `service_manager_stopTimeout` (for deployments with large in-flight
batches), raise the grace period above the new value at the same time.

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
