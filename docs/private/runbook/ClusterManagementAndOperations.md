

# Cloud Services Operation Runbook

## Introduction

This document describes the processes involved in configuring, deploying, and managing the Teranode BSV services using Docker, Kubernetes (k8s), and AWS. It covers the initial setup, common commands, and troubleshooting steps necessary for efficient microservice operation.

**Note:** This document has been created and tested using macOS. Some of the commands will differ for other operating systems.

## Docker and Kubernetes Setup

- **Dockerfile Location**: Within the GitHub project repository, the Dockerfile is located at `./Dockerfile`. This file specifies all requirements for the core set of Teranode microservices, including necessary software packages and environment variables.
- **Compilation Process**: The Dockerfile builds a single executable binary named `ubsv.run` to be included in the image.
- **Deployment to Kubernetes**: The image, including the `ubsv.run` binary, is deployed across our Kubernetes instances. This ensures the binary is consistently deployed and managed across all instances.
- **Execution by Kubernetes**: Kubernetes executes a specific command for each microservice application, as defined in their respective Kubernetes manifest files (e.g., Deployment or Pod specifications). This flexibility allows a single binary to be used in multiple contexts by leveraging symbolic links (symlinks) to point to `ubsv.run`.
- **Filesystem Configuration Example**:
    ```bash
    root@blockassembly1-7874b7bf7c-668bk:/app# ls -ltr
    total 141072
    ...
    -rwxr-xr-x 1 root root 125468128 Jan 29 23:37 ubsv.run
    lrwxrwxrwx 1 root root         8 Jan 29 23:38 utxostoreblaster.run -> ubsv.run
    ...
    ```
  In the above filesystem snapshot from a Kubernetes pod, notice how all applications refer to the same `ubsv.run` binary via symlinks. This setup enhances maintainability and efficiency by centralizing the application logic in a single binary, while symlinks provide the flexibility to execute different aspects of the binary as needed for each microservice.

**Technical Detailing**: Kubernetes interprets these commands and symlinks through the `command` and `args` fields in the container spec within a Pod's manifest.

### Service YAML Files

For each microservice, we maintain several YAML files that define its behavior within a Kubernetes cluster. These files include:

- **Application YAML Configuration Files**: These files manage the deployment setup of containerized applications. For example, `utxo-blaster.yaml` specifies the deployment details for the UTXO Blaster service. Key fields within these files include:
  - `name`: Serves as a base prefix for application naming within Kubernetes.
  - `REPO:IMAGE:TAG`: Specifies the Docker image to be used. This reference is updated with the actual image name during the CI build process.
  - `command`: Indicates the command to be executed within the Docker container. For instance, `utxo-blaster.yaml` might specify `./utxostoreblaster.run` as the command, ensuring that the corresponding executable is available within the Docker image.
  - `role`: Defines which Kubernetes nodes can run the application, based on assigned roles. Node roles can be checked with the command `$ kubectl get node -L role`.
  - `replicas`: Determines the number of application instances to start within the service. A setting of 0 requires manual service startup.
  - `volumeMounts` (optional): Specifies volume configurations, such as for `lustre` volumes, enabling detailed storage setup. For example, in `blockassembly.yaml`, a volume mount might be defined as follows:
    ```
    volumes:
      - name: lustre-storage
        persistentVolumeClaim:
          claimName: lustre-pvc
    ```
    Here, `lustre-pvc` references a specific PersistentVolumeClaim defined in `lustre-pvc.yaml`, indicating a Lustre filesystem storage configuration.

- **Volume YAML Configuration Files**: These files define persistent volumes for storage purposes. A `lustre-pvc.yaml` file, for instance, would specify the configuration for a Lustre filesystem storage volume.

- **Service YAML Configuration Files**: Detail the Kubernetes service configuration for running specific containerized applications. For example, `utxo-blaster-service.yaml` outlines the service setup for the UTXO Blaster application, including port mappings between the internal and external interfaces.

- **Traefik Ingress YAML Configuration Files**: As part of Traefik's Kubernetes CRDs, these files facilitate advanced routing and ingress management, leveraging Traefik's capabilities as an edge router. An example file, `asset-grpc-ingress.yaml`, would define ingress rules for gRPC assets.

- **Kustomize Customization YAML Files**: Utilizing Kustomize, a built-in tool in Kubernetes since version 1.14, these files allow for the customization of Kubernetes configurations based on templates. The CI process employs Kustomize to adjust settings such as `REPO:IMAGE:TAG` and to apply specific ingress rules or modify application names and hosts to suit different environments or nodes within those environments.

- **Deployment Scripts**: Central to the deployment process is the `deploy-to-region.yaml` script, which uses Kustomize to generate the final set of Kubernetes configuration files tailored for specific deployment environments.

### Settings

Microservice configurations can be influenced by settings. There are 2 ways to provide settings:

* Via settings in the `settings.conf` and `settings_local.conf` files, embedded within the Docker image.
* By Kubernetes pod-specific settings. These settings can override configurations defined in the `.conf` files, allowing for dynamic adjustment based on the deployment environment or specific operational requirements.

### Kubernetes Resolver for gRPC

* [Kubernetes Resolver for gRPC](../../../k8sresolver/README.md)


## How to

### AWS CLI Configuration

- Install the AWS Command Line Interface (CLI) via Homebrew: `brew install awscli`. Check the [AWS CLI Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) for other installation methods.
- Run `aws configure` to set up the AWS Command Line Interface (CLI).
- Enter the `AWS Access Key ID`, `AWS Secret Access Key`, `Default region name` (e.g., `eu-north-1`), and `Default output format` as required.
  - Please check with your DevOPS team for the specific credentials available to you.


### Kubernetes (k8s) Cluster Access

1. **Kubectl Installation**:
   - Install kubectl via Homebrew: `brew install kubectl`.

2. **Configure kubeconfig for EKS**:
   - Use the `aws eks update-kubeconfig` command to configure kubectl to interact with your Amazon EKS clusters.
   - Example: `aws eks update-kubeconfig --name aws-ubsv-playground --region <region>`
     - For example, the staging environment supported regions include `ap-south-1`, `eu-west-1`, and `us-east-1`.
   - Verify the configuration with `kubectl config view`. Alternatively, you can verify the raw data in the `~/.kube/config` file.

3. **Zsh Configuration**:
- Download the `k8s_shortcuts.sh` shortcuts file from the shared repository and place it in the home directory.

```bash
cp docs/private/runbook/.k8s_shortcuts.sh ~/.k8s_shortcuts.sh
```

- Add Kubernetes shortcuts to `.zprofile` for ease of use.

```bash

{
echo ""
echo "# Load k8s shortcuts"
echo "autoload -Uz compinit"
echo "compinit"
echo "source ~/.k8s_shortcuts.sh"
} >> ~/.zprofile
```

- Please read more about the shortcuts [here](k8Shortcuts.md).


### Deployment and Scaling

#### **Service Start Order**:

The Teranode services must be started in a specific order, as follows:

1. Blockchain Server.
2. Asset Server.
3. Block Validation.
4. Block Assembly.
5. Propagation Server.
6. P2P Service.
7. The rest of services.

Failing to start services in the right order will lead to errors and incorrect behaviour.

Service restarts or downtime can cause the node to be left in an inconsistent state. For example, the Block Assembly will miss any transaction received during a downtime, being unable to promptly recover.


#### Lustre

Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. The system is designed to support high-performance, large-scale data storage and workloads, widely used in environments that require fast and efficient access to large volumes of data across many nodes.

Teranode microservices make use of the Lustre file system in order to share information related to subtrees, eliminating the need for redundant propagation of subtrees over grpc or message queues.

![lustre_fs.svg](..%2F..%2Flustre_fs.svg)

As seen in the diagram above, the Block Validation, Block Assembly and Asset Services share a lustre file system storage. The data is ultimately persisted in AWS S3.


## Key Kubernetes Commands Documentation

#### Environment Switching and Namespace Management

1. **Environment Shortcuts**:
- Define aliases for switching between environments (e.g., `m1`, `m2`, `m3`) in `.zprofile`.
  - Please check with your DevOPS team for the specific environments available to you.

2. **Namespace Configuration**:
- Use `kcn` command to switch Kubernetes namespaces easily.
- Example:

```bash
kcn m1
```


#### Viewing Pods in a Specific Cluster

- **`kgp`**: Displays all current services running in a cluster.

#### Listing All Services

- **`kgpa`**: Lists all services across all namespaces.

#### Monitoring Pod Resource Usage

- **`kubectl top pods`**: Shows the resource usage for pods.

- **`watch -n 3 kubectl top pods`**: Runs `kubectl top pods` every 3 seconds, providing a real-time view of pod resource usage.

#### Starting Services

- **`ksd {service} --replicas=n`**: Starts a service.

Examples:

- ```ksd coinbase1 --replicas=1```

- ```ksd miner2 --replicas=1```

- ```ksd propagation1 --replicas=13```

- ```ksd tx-blaster1 --replicas=11```

#### Checking the Number of Service Instances

- **`k get node -L role | grep prop | wc -l`**: Counts the number of nodes labeled with the role `prop`, useful for assessing the scale of the propagation service within the cluster.

#### Viewing Logs

- **`kl {pod}`**:

Example:
- `kl tx-blaster1-234234`: Views the logs for a specific `tx-blaster1` pod.

#### Resetting a Service

- **To reset a Service**:
  - If you're experiencing issues with a service not functioning as expected, you might attempt to delete all pods associated with a namespace to force them to restart. An example (for the p2pbootstrap namespace) can be seen here:
    ```sh
    kubectl delete pod -n p2pbootstrap --all
    ```
  - This command deletes all pods in the namespace `p2pbootstrap`, which should cause them to be recreated based on their deployment or stateful set configurations.

- **To reset a specific service in all namespaces**:
  - Example to reset the p2p service can be seen here:
    ```sh
    kubectl delete pod p2p1
    ```

#### Viewing Traefik Namespaces

- **`kpg -n traefik`**: To view pods within the Traefik namespace, ensuring traffic management components are running as expected.

#### Accessing the Shell for any Service


- You can open an interactive terminal (`bash`) in a specified pod with the `keti {pod} -- bash` command. For example:


```bash
# Get the list of pods for the AWS node you are connected to
$  kubectl get pod

# Access one specific microservices, out of the list of pods obtained in the previous step
$   keti blockassembly1-7874b7bf7c-668bk --bash

$   ls -ltr
```

The results will look similar to the below:

```bash
root@blockassembly1-7874b7bf7c-668bk:/app# ls -ltr
total 141072
-rw-r--r-- 1 root root    161960 Jan  8  2021 libsecp256k1.so.0.0.0
-rwxr-xr-x 1 root root  18768613 Dec  7 23:59 dlv
-rw-rw-r-- 1 root root         0 Jan 25 00:12 settings.conf
drwxr-xr-x 2 root root       116 Jan 25 00:12 certs
-rw-rw-r-- 1 root root     49314 Jan 29 14:30 settings_local.conf
-rwxr-xr-x 1 root root 125468128 Jan 29 23:37 ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 utxostoreblaster.run -> ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 propagationblaster.run -> ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 chainintegrity.run -> ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 blockassemblyblaster.run -> ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 blaster.run -> ubsv.run
lrwxrwxrwx 1 root root         8 Jan 29 23:38 s3blaster.run -> ubsv.run
lrwxrwxrwx 1 root root        21 Jan 29 23:38 libsecp256k1.so.0 -> libsecp256k1.so.0.0.0
lrwxrwxrwx 1 root root        21 Jan 29 23:38 libsecp256k1.so -> libsecp256k1.so.0.0.0
lrwxrwxrwx 1 root root         8 Jan 29 23:38 aerospiketest.run -> ubsv.run

```




#### Managing Persistent Storage

- **`k get pv`**: Lists all Persistent Volumes (PVs) in the cluster, essential for managing storage resources.

- **`k describe pvc {storage}`**: Provides detailed information about a Persistent Volume Claim (PVC).

Example:
```bash
# Provides detailed information about the Persistent Volume Claim (PVC) named `subtree-lustre-pv`
k describe pvc subtree-lustre-pv
```

#### Forwarding Ports

- **To forward a pod port to localhost**:
  - Set the Kubernetes configuration file:
    ```sh
    export KUBECONFIG=~/.kube/config
    ```
  - Forward the port:
    ```sh
    kubectl port-forward asset1-63453452352-23452 8090:8090
    ```
    This command forwards the port `8090` from the specified `asset1` pod to the



#### Overriding pod settings

While the different application settings are provided in the `settings.conf` and `settings_local.conf`, users can modify them directly in Kubernetes.

To do so, you have to edit the {application}.run file for a given pod. The example below shows how to access the settings in a Block Assembly application pod.

```bash
ked block-assembly-213131 -- vi block-assembly.run
# Modify the specific config setting, and save
```

After a change, the service will automatically restart.
