# Updating Teranode to a New Version - Kubernetes Operator


With the BSVA Catalog installation method, updates are typically handled automatically for minor releases. However, for major version updates or to change the update policy, follow these steps:

1. **Check Current Version**

```
kubectl get csv -n teranode-operator
```

2. **Update the Subscription**

If you need to change the channel or update behavior, edit the subscription:

```
kubectl edit subscription teranode-operator -n teranode-operator
```

You can modify the

```
channel
```

or

```
installPlanApproval
```

fields as needed.

3. **Monitor the Update Process**

Watch the ClusterServiceVersion (CSV) and pods as they update:

```
kubectl get csv -n teranode-operator -w
kubectl get pods -n teranode-operator -w
```

4. **Verify the Update**

Check that all pods are running and ready:

```
kubectl get pods -n teranode-operator
```

5. **Check Logs for Any Issues**

```
kubectl logs deployment/teranode-operator-controller-manager -n teranode-operator
```



**Important Considerations:**
* **Data Persistence**: The update process should not affect data stored in PersistentVolumes. However, it's always good practice to backup important data before updating.
* **Configuration Changes**: Check the release notes or documentation for any required changes to ConfigMaps or Secrets.
* **Custom Resource Changes**: Be aware of any new fields or changes in the Cluster custom resource structure.
* **Database Migrations**: Some updates may require database schema changes. The operator handles this automatically.
* Some updates may require manual intervention, especially for major version changes. Always refer to the official documentation for specific update instructions



**After the update:**

* Monitor the system closely for any unexpected behavior.

* If you've set up Prometheus and Grafana, check the dashboards to verify that performance metrics are normal.

* Verify that all services are communicating correctly and that the node is in sync with the network.



**If you encounter any issues during or after the update:**
* Check the specific pod logs for error messages:
```
kubectl logs <pod-name>
```

* Check the operator logs for any issues during the update process:
```
kubectl logs -l control-plane=controller-manager -n <operator-namespace>
```

* Consult the release notes for known issues and solutions.

* If needed, you can rollback to the previous version by applying the old Cluster resource and downgrading the operator.

* Reach out to the Teranode support team for assistance if problems persist.



Remember, the exact update process may vary depending on the specific changes in each new version. Always refer to the official update instructions provided with each new release for the most accurate and up-to-date information.
