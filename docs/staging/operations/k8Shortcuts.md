
# Kubernetes Command Runbook Reference

## Introduction

This runbook serves as a comprehensive reference for managing Kubernetes resources using a set of aliases, functions, and configurations intended to streamline daily operations. It assumes the user has basic knowledge of Kubernetes (`kubectl`) and shell scripting.

## Prerequisites

- `kubectl` installed and accessible from the command line.
- Zsh shell with access to `autoload` and `compinit` for command completion.

## Aliases

### General Shortcuts

- **`k`**: Alias for `kubectl`.

### Applying and Managing Kubernetes Resources

- **`kaf`**: Stands for "kubectl apply -f". This command applies a configuration file or directory of files to your cluster, creating or updating resources.

### Executing Commands Across All Namespaces

- **`kca`**: A function that wraps `kubectl` commands to run them against all namespaces. Useful for operations that need to be performed cluster-wide, without specifying a namespace each time.

### Interactive Terminal Access

- **`keti`**: Stands for "kubectl exec -t -i". This command is used to execute an interactive terminal session on a container within a pod. It's invaluable for debugging or managing applications directly within their running environment.

### Configuration and Context Management

- **`kcuc`**: "kubectl config use-context". Switches the current kubectl context, effectively changing the cluster you're interacting with.
- **`kcsc`**: "kubectl config set-context". Modifies kubeconfig files, which can alter how kubectl connects to clusters.
- **`kcdc`**: "kubectl config delete-context". Removes a specified context from the kubeconfig file.
- **`kccc`**: "kubectl config current-context". Displays the current context without changing it.
- **`kcgc`**: "kubectl config get-contexts". Lists all contexts saved in the kubeconfig file.

### Resource Deletion

- **`kdel`, `kdelf`**: Shortcuts for deleting Kubernetes resources. The former deletes resources by name, and the latter deletes resources defined in a file.

### Pod Management

- **`kgp`**: "kubectl get pods". Lists pods in the current namespace.
- **`kgpa`**: Lists pods across all namespaces.
- **`kgpw`, `kgpwide`**: Variants of `kgp` that watch for changes in real-time or display extended information, respectively.
- **`kep`, `kdp`, `kdelp`**: Edit, describe, or delete pods. These commands are essential for pod lifecycle management.
- **`kgpl`, `kgpn`**: Get pods by label or namespace, providing a filtered view of pods based on specific criteria.

### Service and Ingress Management

- **`kgs`, `kgsa`**: List services in the current or all namespaces.
- **`kes`, `kds`, `kdels`**: Edit, describe, or delete services.
- **`kgi`, `kgia`**: List ingress resources, crucial for managing access to services from outside the cluster.
- **`kei`, `kdi`, `kdeli`**: Edit, describe, or delete ingress resources.

### Namespace and Configuration Management

- **`kgns`**: "kubectl get namespaces". Lists all namespaces in the cluster.
- **`kens`, `kdns`, `kdelns`**: Edit, describe, or delete namespaces.
- **`kgcm`, `kgcma`**: List ConfigMaps, which are key-value pairs used for configuration.
- **`kecm`, `kdcm`, `kdelcm`**: Edit, describe, or delete ConfigMaps.

### Secret Management

- **`kgsec`, `kgseca`**: List secrets, which store sensitive data like passwords or tokens.
- **`kdsec`, `kdelsec`**: Describe or delete secrets.

### Deployment, StatefulSet, and DaemonSet Management

- **`kgd`, `kgda`**: List deployments, a key resource for managing applications.
- **`ked`, `kdd`, `kdeld`**: Edit, describe, or delete deployments.
- Similar patterns apply for StatefulSets (`kgss`, `kess`, `kdss`, `kdelss`) and DaemonSets (`kgds`, `keds`, `kdds`, `kdelds`), which manage stateful applications and node-level services, respectively.

### Rollout and Debugging

- **`kpf`**: "kubectl port-forward". Forwards one or more local ports to a pod, facilitating debugging and local access.
- **`kl`, `klf`**: View logs of a pod, with the `-f` flag tailing the log output.
- **`kcp`**: "kubectl cp". Copies files and directories to and from containers in pods.
- **`kgno`**, **`keno`**, **`kdno`**, **`kdelno`**: Manage and get information about cluster nodes.

### Advanced Functionality

- **`kres`**: A custom function to refresh an environment variable across resources, showcasing

the flexibility of using shell functions to extend `kubectl`'s capabilities.

These aliases and functions are designed to streamline Kubernetes workflows, making daily tasks more efficient and reducing the cognitive load of remembering complex command syntax.
