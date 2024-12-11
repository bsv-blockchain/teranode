# Configuring Kubernetes Operator Teranode

## Index

- [Configuring Setting Files](#configuring-setting-files)
- [Optional vs Required services](#optional-vs-required-services)


## Configuring Setting Files



The following settings can be configured in the custom operator ConfigMap.



**Service Ports**

```
PROPAGATION_GRPC_PORT=8084
PROPAGATION_HTTP_PORT=8833
VALIDATOR_GRPC_PORT=8081
BLOCK_ASSEMBLY_GRPC_PORT=8085
SUBTREE_VALIDATION_GRPC_PORT=8086
BLOCK_VALIDATION_GRPC_PORT=8088
BLOCK_VALIDATION_HTTP_PORT=8188
BLOCKCHAIN_GRPC_PORT=8087
ASSET_GRPC_PORT=8091
ASSET_HTTP_PORT=8090
P2P_HTTP_PORT=9906
```

**Service Addresses**

```
propagation_grpcAddresses.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_grpcAddress.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_httpAddresses.docker.m=http://ubsv-propagation:${PROPAGATION_HTTP_PORT}
validator_grpcAddress.docker.m=localhost:${VALIDATOR_GRPC_PORT}
blockassembly_grpcAddress.docker.m=ubsv-blockassembly:${BLOCK_ASSEMBLY_GRPC_PORT}
subtreevalidation_grpcAddress.docker.m=ubsv-subtreevalidation:${SUBTREE_VALIDATION_GRPC_PORT}
blockvalidation_httpAddress.docker.m=http://ubsv-blockvalidation:${BLOCK_VALIDATION_HTTP_PORT}
blockvalidation_grpcAddress.docker.m=ubsv-blockvalidation:${BLOCK_VALIDATION_GRPC_PORT}
blockchain_grpcAddress.docker.m=ubsv-blockchain:${BLOCKCHAIN_GRPC_PORT}
asset_grpcListenAddress.docker.m=:${ASSET_GRPC_PORT}
asset_grpcAddress.docker.m=ubsv-asset:${ASSET_GRPC_PORT}
asset_httpAddress.docker.m=http://ubsv-asset:${ASSET_HTTP_PORT}${asset_apiPrefix}
p2p_httpListenAddress.docker.m=:${P2P_HTTP_PORT}
p2p_httpAddress.docker.m=ubsv-p2p:${P2P_HTTP_PORT}
```

**Database Connections**

```
utxostore.docker.m=aerospike://aerospike:3000/test?WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8&expiration=300
utxoblaster_utxostore_aerospike.docker.m=aerospike://editor:password1234@aerospike:3000/utxo-store?WarmUp=0&ConnectionQueueSize=640&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=64&expiration=900
blockchain_store.docker.m=postgres://miner1:miner1@postgres:5432/ubsv1
```

**P2P Configuration**

```
p2p_static_peers.m=
```



For the purposes of the alpha testing, it is recommended not to modify any settings unless strictly necessary.


## Optional vs Required services

While most services are required for the proper functioning of Teranode, some services are optional and can be disabled if not needed. The following table provides an overview of the services and their status:

| Required          | Optional          |
|-------------------|-------------------|
| Asset Server      | Block Persister   |
| Block Assembly    | UTXO Persister    |
| Block Validator   |                   |
| Subtree Validator |                   |
| Blockchain        |                   |
| Propagation       |                   |
| P2P               |                   |
| Legacy Gateway    |                   |

The Block and UTXO persister services are optional and can be disabled. If enabled, your node will be in Archive Mode, storing historical block and UTXO data.
As Teranode does not retain historical transaction data, this data can be useful for analytics and historical lookups, but comes with additional storage and processing overhead.
Additionally, it can be used as a backup for the UTXO store.
