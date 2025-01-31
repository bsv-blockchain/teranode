# Docker Compose - Starting and Stopping Teranode

Last modified: 29-January-2025

## Index

- [Starting the Node](#starting-the-node)
- [6.1.2. Stopping the Node](#612-stopping-the-node)


## Starting the Node

Starting your Teranode instance in Kubernetes involves deploying the Cluster custom resource, which the operator will use to create and manage all necessary services. Follow these steps to start your node:

1. **Pre-start Checklist:**
- Ensure your Kubernetes cluster is up and running.
- Verify that the Teranode operator is installed and running.
- Check that your `teranode_your_cluster.yaml` file is properly configured.
- Ensure any required ConfigMaps or Secrets are in place.

2. **Navigate to the Teranode Configuration Directory:**
```
cd /path/to/teranode-config
```

3. **Apply the Cluster Custom Resource:**

```
kubectl apply -f teranode_your_cluster.yaml
```
This command creates or updates the Teranode Cluster resource, which the operator will use to deploy all necessary components.

4. **Verify Resource Creation:**

```
kubectl get clusters
```
Ensure your Cluster resource is listed and in the process of being created.

5. **Monitor Pod Creation:**
```
kubectl get pods -w
```
Watch as the operator creates the necessary pods for each Teranode service.

6. **Check Individual Service Logs:**
If a specific service isn't starting correctly, check its logs:
```
kubectl logs <pod-name>
```
Replace <pod-name> with the name of the specific pod you want to investigate.

7. **Access Monitoring Tools:**
- If you've set up Grafana and Prometheus, access them through their respective Service or Ingress endpoints.
- Use `kubectl port-forward` if needed to access these tools locally.

8. **Troubleshooting:**
- If any pod fails to start, check its logs and events:
```
kubectl describe pod <pod-name>
```
- Ensure all required ConfigMaps and Secrets are correctly referenced in your Cluster resource.
- Verify that all PersistentVolumeClaims are bound and have the correct storage class.

9. **Post-start Checks:**
- Verify that all Services are created and have the correct endpoints:
```
kubectl get services
```
- Check that any configured Ingress resources are properly set up:
```
kubectl get ingress
```
- Ensure all microservices are communicating properly with each other by checking their logs for any connection errors.

Remember, the initial startup may take some time, especially if this is the first time starting the node or if there's a lot of blockchain data to sync. Be patient and monitor the logs for any issues.

For subsequent starts after the initial setup, you typically only need to ensure the Cluster resource is applied, unless you've made configuration changes or updates to the system.



## 6.1.2. Stopping the Node

Properly stopping your Teranode instance in Kubernetes is crucial to maintain data integrity and prevent potential issues. Follow these steps to safely stop your node:

1. **Navigate to the Teranode Configuration Directory:**
```
cd /path/to/teranode-config
```

2. **Graceful Shutdown:**
To stop all services defined in your Cluster resource:
```
kubectl delete -f teranode_your_cluster.yaml
```
This command tells the operator to remove all resources associated with your Teranode instance.

3. **Verify Shutdown:**
Ensure all pods are being terminated:
```
kubectl get pods -w
```
Watch as the pods are terminated. This may take a few minutes.

4. **Check for Any Lingering Resources:**
```
kubectl get all,pvc,configmap,secret -l app.kubernetes.io/part-of=teranode-operator
```
Verify that no Teranode-related resources are still present.

5. **Stop Specific Services (if needed):**
If you need to stop only specific services, you can scale down their deployments:
```
kubectl scale deployment <deployment-name> --replicas=0
```
Replace <deployment-name> with the specific deployment you want to stop.

6. **Forced Deletion (use with caution!):**
If resources aren't being removed properly:
```
kubectl delete -f teranode_v1alpha1_cluster.yaml --grace-period=0 --force
```
This forces deletion of resources. Use this only if the normal deletion process is stuck.

7. **Cleanup (optional):**
To remove any orphaned resources:
```
kubectl delete all,pvc,configmap,secret -l app.kubernetes.io/part-of=teranode-operator
```
Be **cautious** with this command as it removes all resources with the Teranode label.



8. **Data Preservation:**
PersistentVolumeClaims are not automatically deleted. Your data should be preserved unless you explicitly delete the PVCs.



9. **Backup Consideration:**
Consider creating a backup of your data before shutting down, especially if you plan to make changes to your setup. You can use tools like Velero for Kubernetes backups.



10. **Monitoring Shutdown:**
Watch the events during shutdown to ensure resources are being removed cleanly:
```
kubectl get events -w
```



11. **Restart Preparation:**
If you plan to restart soon, no further action is needed. Your PersistentVolumeClaims and configurations will be preserved for the next startup.



Remember, always use the graceful shutdown method (deleting the Cluster resource) unless absolutely necessary to force a shutdown. This ensures that all services have time to properly close their connections, flush data to disk, and perform any necessary cleanup operations.



The operator will manage the orderly shutdown of services, but be prepared to manually clean up any resources that might not be removed automatically if issues occur during the shutdown process.
