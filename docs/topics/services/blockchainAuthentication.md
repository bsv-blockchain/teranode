# Blockchain service authentication

The Blockchain service requires `grpc_admin_api_key` in every environment, including
local development. It refuses to start when the key is empty, a known placeholder,
has surrounding whitespace, or is shorter than 16 characters. Settings loaded from
configuration trim surrounding whitespace before validation. Use a random secret of
32 or more characters and supply the same value to every service and CLI client.

All BlockchainAPI and PeerRegistryService RPCs require `x-api-key`, including reads
and the Subscribe stream. The only anonymous RPC is HealthGRPC. HTTP `/health` is
also anonymous; `/invalidate/:hash` and `/revalidate/:hash` accept authenticated POST
requests only. Existing GET-based admin scripts must be updated. Health checks do
not prove that a client's credential matches: verify a protected operation too.

## Deployment and development

Generate a key locally and export it before starting the node:

```sh
export grpc_admin_api_key="$(openssl rand -hex 32)"
make dev
```

Reuse that value in every terminal/process belonging to this deployment. Keep it in
an untracked local environment file or secret store if it must survive restarts;
do not regenerate it separately for each service and do not commit it. The Docker
base compose file requires this environment variable (or a `.env` entry) and passes
it to all services. For Kubernetes, put it in the `teranode-operator-secrets` Secret
referenced by the Cluster's `spec.envFrom`, including for operator/CLI processes.
The test compose stacks use a fixed test-only key; never reuse it in a deployment.

## Upgrade order

1. Provision the shared key to all services and operator clients.
2. Upgrade all Blockchain and PeerRegistry callers to versions that attach the key
   on unary calls and streams. Older Blockchain servers accept these clients.
3. Upgrade Blockchain last, then verify authenticated state access, subscriptions,
   peer-registry updates, and block processing.

If processes cannot be upgraded separately, perform a coordinated restart with the
key configured everywhere. An older client can pass HealthGRPC but fail all useful
operations against the new server. Leaving the key empty does not provide a
migration bypass: Blockchain refuses startup. A mismatched key returns
`Unauthenticated`. Rotation requires coordinating the shared value across the fleet;
this change does not add simultaneous support for two keys.

## Transport and reflection

API-key authentication does not encrypt traffic. Use verified TLS across untrusted
networks (`security_level_grpc=2` verifies the server; level 3 additionally verifies
client certificates), and keep internal service listeners off public networks.
The Blockchain HTTP admin listener should be reached only over a trusted connection
or a TLS-terminating internal proxy. This change does not add HTTP TLS.

`grpc_enable_reflection` defaults to false for all services using StartGRPCServer.
Enable it only for development diagnostics. Blockchain reflection requires the key
on both v1 and v1alpha reflection streams. Generated clients need no reflection.

## Scope

This change closes the unauthenticated Blockchain control plane. It retains the
existing P2P/Legacy policy and does not provide per-service roles: possession of the
shared key grants access to all Blockchain operations. Stored-checkpoint validation,
state-store namespaces, and workload identity remain separate changes.
