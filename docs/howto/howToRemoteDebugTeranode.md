
# Remotely Debugging a Teranode Operator

For advanced users who need to debug a running Teranode operator, follow these steps:

1. **Edit the Cluster Custom Resource (CR)**:
Use the following command, replacing `mainnet-1` with your namespace if different:

```bash
kubectl edit clusters.teranode.bsvblockchain.org -n mainnet-1 mainnet-1
```

2. **Modify the Service Configuration**:
Find the service you want to debug (e.g., legacy) and override the `command` key under `deploymentOverrides`:

```yaml
command:

- ./dlv
- --listen=:4040
- --continue
- --accept-multiclient
- --headless=true
- --api-version=2
- exec
- ./teranode.run
- --
```

Note: If you need to debug multiple services, add this command to all necessary services.

3. **Port Forward the Debugger**:
You have two options:

a. Using kubectl (easier for a single port):
```bash
kubectl port-forward -n mainnet-1 <replace_with_pod_name> 4040:4040
```
This will provide you with an IP:PORT (often localhost) to access the debugger.

b. Using kubefwd (useful for multiple services):
```bash
sudo kubefwd svc -n mainnet-1
```
This maps ports with service names (e.g., asset:4040, legacy:4040, blockchain:4040).

4. **Configure VS Code**:

    - Set up VS Code to connect to the remote debugger now running locally on your computer.
    - Avoid using the "cursor" feature, as it may cause connection issues.
    - If using kubefwd, ensure your debugger IP points to the correct service (e.g., use `asset:4040` for the asset service).

5. **Start Debugging**:

    - Add breakpoints as needed.
    - Begin your debugging session.

Note: When debugging multiple services, make sure to use the correct local DNS (e.g., `asset:4040`) for each service you're debugging.
