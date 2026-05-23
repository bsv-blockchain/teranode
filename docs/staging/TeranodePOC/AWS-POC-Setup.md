## Teranode - POC Environment

### 1. Teranode AWS Setup

![AWS - POC - Setup.png](AWS%20-%20POC%20-%20Setup.png)

On February 2024, the BSV team started a multi-month test exercise (the proof-of-concepts or POC). The objective of the exercise is to prove that Teranode BSV can scale to process millions of transactions per second.

For the purpose of this test, Amazon AWS was chosen as the cloud server provider. Three (3) nodes were created in 3 separate AWS regions. Each node is composed of a number of server instances. The purpose of this document is to describe the setup of each node.

### 2. Instance Details

The table below provides a summary of the server groups, the number of instances per group, and their technical specification.

| Instance Group      | Instances | Instance Type | vCPUs | Memory (GiB) | Network (Gbps) | Processor                             |
|---------------------|-----------|---------------|-------|--------------|----------------|---------------------------------------|
| Asset-sm            | 1         | c6in.24xlarge | 96    | 192          | 150            | Intel Xeon 8375C (Ice Lake)           |
| Block Assembly      | 1         | m6in.32xlarge | 128   | 512          | 200            | Intel Xeon 8375C (Ice Lake)           |
| Block Validation    | 1         | r7i.48xlarge  | 192   | 1536         | 50             | Intel Xeon Scalable (Sapphire Rapids) |
| Main Pool           | 3         | c6gn.12xlarge | 48    | 96           | 75             | AWS Graviton2 Processor               |
| Propagation Servers | 16        | c6in.16xlarge | 64    | 128          | 100            | Intel Xeon 8375C (Ice Lake)           |
| Proxies             | 6         | c7gn.8xlarge  | 32    | 64           | 100            | AWS Graviton3 Processor               |
| TX Blasters         | 13        | c6gn.8xlarge  | 32    | 64           | 50             | AWS Graviton2 Processor               |
| Aerospike           | 10        | i3en.24xlarge | 96    | 768          | 100            | Intel Xeon Platinum 8175              |

AWS EKS (Kubernetes) is used to run services in all instances. Aerospike has been run in and out of Kubernetes as part of the test exercise.

Please note that exact instance count under the Main Pool, Propagation Servers, Proxies, TX Blaster and Aerospike groups can vary during the test.

Additionally, a Teranode node can have a number of other servers and containers related to secondary services, such as monitoring (prometheus, datadog). Such secondary instances are out of the scope of this document.

### 3. Instance Group Definitions

Here's a structured table summarizing the instance groups, their descriptions, and purposes based on the provided details:

| Instance Group      | Description                  | Purpose                                                                           |
|---------------------|------------------------------|-----------------------------------------------------------------------------------|
| Proxies             | Ingress Proxy Servers        | Managing region-to-region ingress traffic with Traefik k8s services.              |
| Asset-sm            | Asset (Small Instance)       | Hosting the Asset Server Teranode microservice.                                   |
| Block Assembly      | Block Assembly               | Running the Block Assembly Teranode microservice.                                 |
| Block Validation    | Block Validation             | Running the Block Validation Teranode microservice.                               |
| Main Pool           | Overlay Services             | Running the p2p, coinbase, blockchain, postgres, faucet, and other microservices. |
| Propagation Servers | Propagation Servers          | A pool of Propagation Teranode microservices.                                     |
| TX Blasters         | TX Blasters                  | A pool of TX Blaster Teranode microservices.                                      |
| Aerospike           | Aerospike                    | A cluster of Aerospike NoSQL DB services.                                         |

### 4. Storage

- **AWS S3** - Configured to work with Lustre filesystem, shared across services, and used for Subtrees and TX data storage.
- **Aerospike** - Used for UTXO and TX Meta data storage.
- **Postgresql** - Within the Main Pool instance group, an instance of Postgres is run. The Blockchain service stores the blockchain data in postgres.

### 5. Performance Metrics

In the context of our AWS-based infrastructure, the primary consideration for specifying instance types has been to meet the high network bandwidth requirements essential for the Teranode services. This approach was adopted following the identification of network bandwidth as a critical bottleneck in previous discovery phases.

However, the services are not fully utilizing the allocated resources in terms of CPU utilization, memory utilization, and disk I/O. There is excess capacity in these areas which may present future opportunities for cost optimization or reallocation of resources to better match actual usage patterns.

### 6. AWS Kubernetes Configuration Notes

When deploying Teranode on AWS with Kubernetes, several configuration files in the `deploy/kubernetes/` directory require AWS-specific customization. The default configurations are designed for local development and must be adapted for AWS EKS deployments.

#### Storage Configuration

⚠️ **Critical:** The following storage configurations must be customized for your AWS environment:

**Storage Classes:**

- Default configurations may use local storage classes
- AWS requires specific StorageClass definitions (`gp2`, `gp3`, or `efs-sc-dynamic`)
- Shared storage (for multiple pods) requires AWS EFS with the EFS CSI driver

**Required Changes:**

1. **Persistent Volumes and Claims** - Must specify appropriate `storageClassName`:
   - `deploy/kubernetes/postgres/postgres-pvc.yaml` - Add `storageClassName: gp2`
   - `deploy/kubernetes/aerospike/aerospike-claim1-persistentvolumeclaim.yaml` - Add `storageClassName: efs-sc-dynamic`

2. **StatefulSets** - Must include `volumeClaimTemplates` with AWS storage classes:
   - `deploy/kubernetes/postgres/postgres.yaml` - Add `volumeClaimTemplates` with `storageClassName: gp2` and `subPath: pgdata`
   - `deploy/kubernetes/aerospike/aerospike-statefulset.yaml` - Add `volumeClaimTemplates` with `storageClassName: gp2`

3. **NFS/EFS Configuration** - For shared storage across pods:
   - `deploy/kubernetes/nfs/pv.yaml` - Replace NFS driver with EFS CSI driver (`efs.csi.aws.com`) and configure `volumeHandle` with your EFS filesystem ID

4. **Teranode Operator CR** - Must include storage configuration:
   - `deploy/kubernetes/teranode/teranode-cr.yaml` - Add `volumeClaimTemplates` with `storageClassName: efs-sc-dynamic` under each service's `deploymentOverrides`

#### Image Pull Policy

⚠️ **Important:** Default configurations may have `imagePullPolicy: Never` which is only suitable for local development with locally-built images.

For AWS deployments:

- Change to `imagePullPolicy: IfNotPresent` (recommended) - pulls only if image not present locally
- Or use `imagePullPolicy: Always` - always pulls latest image (slower but ensures freshness)

Files affected:

- `deploy/kubernetes/teranode/teranode-cr.yaml` - Update all service `deploymentOverrides`

#### Environment-Specific Considerations

**Storage Performance:**

- `gp2`: General purpose SSD, baseline IOPS (3 IOPS/GB)
- `gp3`: Newer general purpose SSD, better performance/cost ratio
- EFS: Required for ReadWriteMany access mode (shared across pods)

**Volume Paths:**

- Ensure volume mount paths match your application's expected data directories
- PostgreSQL requires `subPath: pgdata` to avoid permission issues with lost+found
- Review all volume mounts in the configmap (e.g., `/data/teranode-operator/external`)

**Cost Optimization:**

- EFS can be expensive for high-throughput workloads
- Consider using local instance storage (ephemeral) for non-critical data
- Monitor EFS usage and adjust performance mode as needed

#### Testing Checklist

Before deploying to AWS, verify:

- [ ] All PVCs specify appropriate AWS storage classes
- [ ] StatefulSets include volumeClaimTemplates with correct storage classes
- [ ] EFS filesystem is created and volumeHandle is configured
- [ ] Image pull policies are set to IfNotPresent or Always
- [ ] Volume mount paths match application expectations
- [ ] PostgreSQL includes subPath configuration
- [ ] ConfigMap paths use absolute paths (e.g., `file:///data` not `file://./data`)
- [ ] Binaries are cross-compiled for target architecture (ARM64 for Graviton, AMD64 for Intel instances)
    - Example: `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 make build-teranode`
    - See [Makefile Documentation](../../howto/makefile.md#environment-variables) for cross-compilation options

For detailed Kubernetes deployment instructions, see the [Kubernetes Installation Guide](../../howto/miners/kubernetes/minersHowToInstallation.md).
