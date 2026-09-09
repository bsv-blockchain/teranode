# Security Best Practices

Last modified: 11-August-2026

## Firewall Configuration

Here are some firewall configuration recommendations:

### 1. Publicly Exposed Ports

Review the ports exposed in the Kubernetes operator configuration file(s) and ensure your firewall is configured to handle these appropriately:

- `9292`: RPC Server. Open to receive RPC API requests.
- `8090`: Asset Server. Open for incoming HTTP asset requests. This should not be exposed directly, but via a reverse proxy or caching mechanism.
- `9905,9906`: P2P Server. Open for incoming connections to allow peer discovery and communication.
- `8098, 8099`: Legacy P2P Service, open for HTTP and gRPC.

### 2. Restricted Ports

Review the ports exposed in the Kubernetes operator configuration file(s) and ensure your firewall is configured to handle these appropriately:

- `9292`: RPC Server. Open to receive RPC API requests. Not open to public access.

### 3. Host Firewall

- Configure your host's firewall to allow incoming connections only on the necessary ports.
- For ports that don't need external access, strictly restrict them to localhost (127.0.0.1) or your internal network.

### 4. External Access

- Only expose ports to the internet that are absolutely necessary for node operation (e.g., P2P and Asset server ports).
- Use strong authentication for any services that require external access. See the section 4.1 of this document for more details.

### 5. Network Segmentation

- If possible, place your Teranode host on a separate network segment with restricted access to other parts of your infrastructure.

### 6. Regular Audits

- Periodically review your firewall rules and exposed ports to ensure they align with your security requirements.

### 7. Service-Specific Recommendations

- **PostgreSQL (5432)**: If you want to expose it, restrict to internal network, never publicly.
- **Kafka (9092, 9093)**: If you want to expose it, restrict to internal network, never publicly.
- **Aerospike (3000)**: If you want to expose it, restrict to internal network, never publicly.
- **Grafana (3005)**: Secure with strong authentication if exposed externally.

### 8. P2P Communication

- Ensure ports 9905 and 9906 are open for incoming connections to allow peer discovery and communication.

!!! info "Important Security Principle"
    Remember, the exact firewall configuration will depend on your specific network setup, security requirements, and how you intend to operate your Teranode. Always follow the principle of least privilege, exposing only what is necessary for operation.

## PostgreSQL Role Privileges

Teranode's PostgreSQL role (`teranode`) owns the `teranode` database and needs no
superuser rights. New deployments provision it as `NOSUPERUSER`.

Existing deployments are not fixed by an update. The role is created by the
`init.sql` in the `postgres-cm0` ConfigMap, which the `postgres` image runs only
when its data directory is empty, and that directory lives on a persistent volume
claim. Upgrading the operator or the Teranode image leaves the role exactly as it
was first created, so a cluster provisioned before this change still runs with a
superuser `teranode` role.

Remediate once, as the `postgres` superuser (adjust namespace and pod name to your
deployment):

```bash
kubectl exec -n teranode-operator postgres-0 -- \
    psql -U postgres -c 'ALTER ROLE teranode NOSUPERUSER;'
```

Verify:

```bash
kubectl exec -n teranode-operator postgres-0 -- \
    psql -U postgres -tAc "SELECT rolname, rolsuper FROM pg_roles WHERE rolname = 'teranode';"
```

Expect `teranode|f`. Object ownership is unaffected — `teranode` still owns its
database and tables, so no regrant is needed and Teranode can be left running.

## Regular System Updates

!!! tip "System Update Recommendations"
    In order to receive the latest bug fixes and vulnerability patches, please ensure you perform periodic system updates, as regularly as feasible. Please refer to the Teranode update process outlined in the Section 6 of this document.
