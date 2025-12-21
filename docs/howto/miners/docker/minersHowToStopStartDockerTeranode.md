# Docker Compose - Starting and Stopping Teranode

Last modified: 22-January-2025

## Index

- [Starting the Node](#starting-the-node)
- [Stopping the Node](#stopping-the-node)

## Starting the Node

Starting your Teranode instance involves initializing all the necessary services in the correct order. The Docker Compose configuration file handles this complexity for us.

This guide assumes you have previously followed the [installation guide](./minersHowToInstallation.md) and have a working Teranode setup.

Follow these steps to **start** your node:

### Step 1: Navigate to the Teranode Directory

Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/mainnet
```

### Step 2: Pull Latest Images (optional, but recommended before each start)

```bash
docker compose pull
```

### Step 3: Start the Teranode Stack

```bash
docker compose up -d
```

This command starts all services defined in the `docker-compose.yml` file in detached mode.

### Step 4: Verify Service Startup

```bash
docker compose ps
```

Ensure all services show a status of "Up" or "Healthy".

### Step 5: Monitor Startup Logs

```bash
docker compose logs -f
```

This allows you to watch the startup process in real-time.

### Step 6: Check Individual Service Logs

If a specific service isn't starting correctly, check its logs. Example:

```bash
docker compose logs -f legacy
docker compose logs -f blockchain
docker compose logs -f asset
```

### Step 7: Verify Network Connections

Once all services are up, ensure the node is responsive:

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc": "1.0", "id": "curltest", "method": "version", "params": []}' -H 'content-type: text/plain;' http://127.0.0.1:9292/
```

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc": "1.0", "id": "curltest", "method": "getinfo", "params": []}' -H 'content-type: text/plain;' http://127.0.0.1:9292/
```

And that it is connected to its peers:

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc": "1.0", "id": "curltest", "method": "getpeerinfo", "params": []}' -H 'content-type: text/plain;' http://127.0.0.1:9292/
```

### Step 8: Check Synchronization Status

Monitor the blockchain synchronization process using the blockchain viewer:

```bash
# Open the blockchain viewer in your browser to view current blockchain state
# http://localhost:8090/viewer
```

Alternatively, query the RPC endpoint for programmatic access:

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc": "1.0", "id": "curltest", "method": "getblockchaininfo", "params": []}' -H 'content-type: text/plain;' http://127.0.0.1:9292/
```

### Step 9: Access Monitoring Tools

- Open Grafana at http://localhost:3005 to view system metrics.
- Check Prometheus at http://localhost:9090 for raw metrics data.

### Step 10: Troubleshooting

- If any service fails to start, check its specific logs and configuration.
- Ensure all required ports are open and not conflicting with other applications.
- Verify that all external dependencies (Kafka, Postgres, Aerospike) are running correctly.

### Step 11: Post-start Checks

Remember, the initial startup may take some time, especially if this is the first time starting the node or if there's a lot of blockchain data to sync. Be patient and monitor the logs for any issues.

For subsequent starts after the initial setup, you typically only need to run the `docker compose up -d` command, unless you've made configuration changes or updates to the system.


## Stopping the Node

Properly stopping your Teranode instance is crucial to maintain data integrity and prevent potential issues. Follow these steps to safely stop your node:

### Step 1: Navigate to the Teranode Directory

Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/mainnet
```

### Step 2: Graceful Shutdown

To stop all services defined in your `docker-compose.yml` file:

```bash
docker compose down
```

This command stops and removes all containers, but preserves your data volumes.

### Step 3: Verify Shutdown

Ensure all services have stopped:

```bash
docker compose ps
```

This should show no running containers related to your Teranode setup.

### Step 4: Check for Any Lingering Processes

```bash
docker ps
```

Verify that no Teranode-related containers are still running.

### Step 5: Stop Specific Services (if needed)

If you need to stop only specific services:

```bash
docker compose stop [service-name]
```

Replace [service-name] with the specific service you want to stop, e.g., teranode-blockchain.

### Step 6: Forced Shutdown (use with caution!)

If services aren't responding to the normal shutdown command:

```bash
docker compose down --timeout 30
```

This forces a shutdown after 30 seconds. Adjust the timeout as needed.

### Step 7: Cleanup (optional)

To remove all stopped containers and free up space:

```bash
docker system prune
```

Be **cautious** with this command as it removes all stopped containers, not just Teranode-related ones.

### Step 8: Data Preservation

The `docker compose down` command doesn't remove volumes by default. Your data should be preserved in the ./data directory.

### Step 9: Backup Consideration

Consider creating a backup of your data before shutting down, especially if you plan to make changes to your setup.

### Step 10: Monitoring Shutdown

Watch the logs during shutdown to ensure services are stopping cleanly:

```bash
docker compose logs -f
```

### Step 11: Restart Preparation

If you plan to restart soon, no further action is needed. Your data and configurations will be preserved for the next startup.

!!! warning "Important: Always Use Graceful Shutdown"
    Remember, **always** use the **graceful shutdown** method (`docker compose down`) unless absolutely necessary to force a shutdown. This ensures that all services have time to properly close their connections, flush data to disk, and perform any necessary cleanup operations.
