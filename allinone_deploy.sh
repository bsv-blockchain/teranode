#!/bin/bash

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null
then
    echo "kubectl could not be found, please install it to continue (brew install kubectl)."
    exit 1
fi

# Check if kustomize is installed
if ! command -v kustomize &> /dev/null
then
    echo "kustomize could not be found, please install it to continue (brew install kustomize)."
    exit 1
fi

# Check if helm is installed
if ! command -v helm &> /dev/null
then
    echo "helm could not be found, please install it to continue (brew install kustomize)."
    exit 1
fi

# Check if yq is installed
if ! command -v yq &> /dev/null
then
    echo "yq could not be found, please install it to continue (brew install yq)."
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

IMAGE_TAG=$(git rev-parse HEAD)
IMAGE_NAME="434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:latest"

REGION=$(kubectl config view --minify --output 'jsonpath={..name}' | cut -d ' ' -f1 | cut -d ':' -f 4)
NAMESPACE=$(kubectl config view --minify --output 'jsonpath={..namespace}')

case $REGION in
    "docker-desktop")
        if [[ $NAMESPACE != "miner-lo-1" ]]; then
            echo "You are in $REGION region with a namespace of $NAMESPACE.  Please switch to miner-lo-1 namespace to continue."
            exit 1
        fi

        TRAEFIK=$(kubectl get namespaces traefik --output 'jsonpath={..name}')
        if [[ -z $TRAEFIK ]]; then
            echo "Traefik is not installed, installing it now..."
            kubectl create namespace traefik
            helm repo add traefik https://traefik.github.io/charts
            helm repo update
            helm install traefik traefik/traefik
        fi

        docker pull $IMAGE_NAME
        if [[ $? -ne 0 ]]; then
            echo "Failed to pull image: $IMAGE_NAME"
            echo "Login to ECR and try again."
            # Force authentication to ECR...
            echo "aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com"
            exit 1
        fi

        ;;
    "eu-west-1")
        if [[ $NAMESPACE != "miner-eu-1" ]]; then
            echo "You are in $REGION region with a namespace of $NAMESPACE.  Please switch to miner-eu-1 namespace to continue."
            exit 1
        fi
        ;;
    "us-east-1")
        if [[ $NAMESPACE != "miner-us-1" ]]; then
            echo "You are in $REGION region with a namespace of $NAMESPACE.  Please switch to miner-us-1 namespace to continue."
            exit 1
        fi
        ;;
    "ap-northeast-1")
        if [[ $NAMESPACE != "miner-eu-1" ]]; then
            echo "You are in $REGION region with a namespace of $NAMESPACE.  Please switch to miner-ap-1 namespace to continue."
            exit 1
        fi
        ;;
    *)
        echo "Unknown region: $REGION"
        exit 1
        ;;
esac

cd $DIR/deploy/k8s/base/allinone-miner

kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:$(git rev-parse HEAD)
# kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:ae32fd77d3f2f747c0787fd04b2ec7931afd32c7


cd $DIR/deploy/k8s/$REGION/allinone

kustomize build . $@

cd $DIR/deploy/k8s/base/allinone-miner

yq eval 'del(.images)' -i kustomization.yaml

cd $DIR
