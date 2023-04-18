To install services in minicube you need to do the following steps.

# Install minikube
brew install minikube

# Start minikube
minikube start

# Install helm
brew install helm

# Install the helm chart
helm install ubsv deploy/minikube

# Check the status of the deployment
kubectl get pods

# Check the logs of the deployment
kubectl logs -f ubsv-ubsv-0

