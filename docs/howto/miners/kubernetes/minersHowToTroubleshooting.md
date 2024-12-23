# How to Troubleshoot Teranode (Kubernetes Operator)

Last modified: 13-December-2024

## Index

- [Health Checks and System Monitoring](#health-checks-and-system-monitoring)
    - [Service Status](#service-status)
    - [Detailed Container/Pod Health](#detailed-containerpod-health)
    - [Configuring Health Checks](#configuring-health-checks)
    - [Viewing Health Check Logs](#viewing-health-check-logs)
    - [Monitoring System Resources](#monitoring-system-resources)
    - [Viewing Global Logs](#viewing-global-logs)
    - [Viewing Logs for Specific Microservices](#viewing-logs-for-specific-microservices)
    - [Useful Options for Log Viewing](#useful-options-for-log-viewing)
    - [Checking Logs for Specific Teranode Microservices](#checking-logs-for-specific-teranode-microservices)
    - [Redirecting Logs to a File](#redirecting-logs-to-a-file)
    - [Check Services Dashboard**](#check-services-dashboard)
- [Recovery Procedures](#recovery-procedures)
    - [Third Party Component Failure](#third-party-component-failure)


## Health Checks and System Monitoring


### Service Status

```bash
kubectl get pods
```
This command lists all pods in the current namespace, showing their status and readiness.


### Detailed Container/Pod Health


```bash
kubectl describe pod <pod-name>
```
This provides detailed information about the pod, including its current state, recent events, and readiness probe results.



### Configuring Health Checks



In your Deployment or StatefulSet specification:

```yaml
spec:
  template:
    spec:
      containers:
      - name: teranode-blockchain
        ...
        readinessProbe:
          httpGet:
            path: /health
            port: 8087
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /health
            port: 8087
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
          initialDelaySeconds: 40
```


### Viewing Health Check Logs


Health check results are typically logged in the pod events:

```bash
kubectl describe pod <pod-name>
```
Look for events related to readiness and liveness probes.



### Monitoring System Resources



* Use `kubectl top` to view resource usage:

```bash
kubectl top pods
kubectl top nodes
```


For both environments:

* Consider setting up Prometheus and Grafana for more comprehensive monitoring.
* Look for services consuming unusually high resources.



### Viewing Global Logs


```bash
kubectl logs -l app.kubernetes.io/part-of=teranode-operator
kubectl logs -f -l app.kubernetes.io/part-of=teranode-operator  # Follow logs in real-time
kubectl logs --tail=100 -l app.kubernetes.io/part-of=teranode-operator  # View only the most recent logs
```



### Viewing Logs for Specific Microservices





```bash
kubectl logs <pod-name>
```



### Useful Options for Log Viewing





* Show timestamps:
```bash
kubectl logs <pod-name> --timestamps=true
```
* Limit output:
```bash
kubectl logs <pod-name> --tail=50
```
* Since time:
```bash
kubectl logs <pod-name> --since-time="2023-07-01T00:00:00Z"
```



### Checking Logs for Specific Teranode Microservices



Replace `[service_name]` or `<pod-name>` with the appropriate service or pod name:

* Propagation Service (service name: `propagation`)
* Blockchain Service (service name: `blockchain`)
* Asset Service (service name: `asset`)
* Block Validation Service (service name: `block-validator`)
* P2P Service (service name: `p2p`)
* Block Assembly Service (service name: `block-assembly`)
* Subtree Validation Service (service name: `subtree-validator`)
* Miner Service (service name: `miner`)
* RPC Service (service name: `rpc`)
* Block Persister Service (service name: `block-persister`)
* UTXO Persister Service (service name: `utxo-persister`)


### Redirecting Logs to a File



```bash
kubectl logs -l app.kubernetes.io/part-of=teranode-operator > teranode_logs.txt
kubectl logs <pod-name> > pod_logs.txt
```

Remember to replace placeholders like `[service_name]`, `<pod-name>`, and label selectors with the appropriate values for your Teranode setup.



### Check Services Dashboard**



Check your Grafana `TERANODE Service Overview` dashboard:



- Check that there's no blocks in the queue (`Queued Blocks in Block Validation`). We expect little or no queueing, and not creeping up. 3 blocks queued up are already a concern.



- Check that the propagation instances are handling around the same load to make sure the load is equally distributed among all the propagation servers. See the `Propagation Processed Transactions per Instance` diagram.



- Check that the cache is at a sustainable pattern rather than "exponentially" growing (see both the `Tx Meta Cache in Block Validation` and `Tx Meta Cache Size in Block Validation` diagrams).



- Check that go routines (`Goroutines` graph) are not creeping up or reaching excessive levels.



## Recovery Procedures



### Third Party Component Failure



Teranode is highly dependent on its third party dependencies. Postgres, Kafka and Aerospike are critical for Teranode operations, and the node cannot work without them.



If a third party service fails, you must restore its functionality. Once it is back, please restart Teranode cleanly, as per the instructions in the Section 6 of this document.




------


Should you encounter a bug, please report it following the instructions in the Bug Reporting section.
