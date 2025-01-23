# Docker Compose - Starting and Stopping Teranode

Last modified: 22-January-2025

## Index

- [Starting the Node](#starting-the-node)
- [Stopping the Node](#stopping-the-node)

## Starting the Node

Starting your Teranode instance involves initializing all the necessary services in the correct order. The Docker Compose configuration file handles this complexity for us.

This guide assumes you have previously followed the [installation guide](../../../../docs/howto/miners/docker/minersHowToInstallation.md) and have a working Teranode setup.

Follow these steps to **start** your node:

1. **Navigate to the Teranode Directory:**

Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/mainnet
```

2. **Pull Latest Images **(optional, but recommended before each start)**:**

```
docker-compose pull
```

4. **Start the Teranode Stack:**

```
docker-compose up -d
```

This command starts all services defined in the `docker-compose.yml` file in detached mode.

5. **Verify Service Startup:**

```
docker-compose ps
```

Ensure all services show a status of "Up" or "Healthy".

6. **Monitor Startup Logs:**

```
docker-compose logs -f
```

This allows you to watch the startup process in real-time.

7. **Check Individual Service Logs:**

If a specific service isn't starting correctly, check its logs. Example:

```bash
docker-compose logs -f legacy
docker-compose logs -f blockchain
docker-compose logs -f asset
```

8. **Verify Network Connections:**
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


9. **Check Synchronization Status:**
Monitor the blockchain synchronization process:

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc": "1.0", "id": "curltest", "method": "getblockchaininfo", "params": []}' -H 'content-type: text/plain;' http://127.0.0.1:9292/
```

10. **Access Monitoring Tools:**

- Open Grafana at http://localhost:3005 to view system metrics.
- Check Prometheus at http://localhost:9090 for raw metrics data.

11. **Troubleshooting:**

- If any service fails to start, check its specific logs and configuration.
- Ensure all required ports are open and not conflicting with other applications.
- Verify that all external dependencies (Kafka, Postgres, Aerospike) are running correctly.

12. **Post-start Checks:**

Remember, the initial startup may take some time, especially if this is the first time starting the node or if there's a lot of blockchain data to sync. Be patient and monitor the logs for any issues.

For subsequent starts after the initial setup, you typically only need to run the `docker-compose up -d` command, unless you've made configuration changes or updates to the system.


## Stopping the Node

Properly stopping your Teranode instance is crucial to maintain data integrity and prevent potential issues. Follow these steps to safely stop your node:

1. **Navigate to the Teranode Directory:**

Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/mainnet
```

2. **Graceful Shutdown:**

To stop all services defined in your `docker-compose.yml` file:

```
docker-compose down
```
This command stops and removes all containers, but preserves your data volumes.

3. **Verify Shutdown:**
Ensure all services have stopped:

```
docker-compose ps
```
This should show no running containers related to your Teranode setup.

4. **Check for Any Lingering Processes:**

```
docker ps
```
Verify that no Teranode-related containers are still running.

5. **Stop Specific Services (if needed):**
If you need to stop only specific services:

```
docker-compose stop [service-name]
```
Replace [service-name] with the specific service you want to stop, e.g., teranode-blockchain.

6. **Forced Shutdown (use with caution!):**
If services aren't responding to the normal shutdown command:

```
docker-compose down --timeout 30
```
This forces a shutdown after 30 seconds. Adjust the timeout as needed.

7. **Cleanup** (optional):
To remove all stopped containers and free up space:

```
docker system prune
```
Be **cautious** with this command as it removes all stopped containers, not just Teranode-related ones.

8. **Data Preservation:**
The `docker-compose down` command doesn't remove volumes by default. Your data should be preserved in the ./data directory.

9. **Backup Consideration:**
Consider creating a backup of your data before shutting down, especially if you plan to make changes to your setup.

10. **Monitoring Shutdown:**
Watch the logs during shutdown to ensure services are stopping cleanly:

```
docker-compose logs -f
```

11. **Restart Preparation:**
If you plan to restart soon, no further action is needed. Your data and configurations will be preserved for the next startup.



Remember, <u>always</u> use the <u>graceful shutdown</u> method (`docker-compose down`) unless absolutely necessary to force a shutdown. This ensures that all services have time to properly close their connections, flush data to disk, and perform any necessary cleanup operations.
