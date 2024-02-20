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


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

usage() {
    echo ""
    echo "Usage:   $0 --image <image_tag> --environment <environment>"
    echo "Example: $0 --image latest --environment allinone"
    echo "         $0 --image 5079e0084db92a0f635e5d8d15df207108e8e401 --environment scaling"
    echo "         $0 --image scaling-v0.1.2 --environment scaling"
    echo ""
    echo "         --filter <filter>  Filter the output of kustomize"
    echo ""
    exit 1
}

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --image)
            IMAGE_TAG="$2"
            shift
            shift
            ;;
        --environment)
            ENVIRONMENT="$2"
            shift
            shift
            ;;
        --filter)
            FILTER="$2"
            shift
            shift
            ;;
        *)
            usage
            ;;
    esac
done

if [[ -z $IMAGE_TAG || -z $ENVIRONMENT ]]; then
    usage
fi

case $ENVIRONMENT in
    "docker-desktop")
        ;;
    "allinone")
        ;;
    "scaling")
        ;;
    *)
        echo "Invalid environment. Please specify 'docker-desktop', 'allinone' or 'scaling'."
        exit 1
        ;;
esac

# Check if the image tag is "latest" or a valid SHA1 hash
if [[ $IMAGE_TAG != "latest" && ! $IMAGE_TAG =~ ^[0-9a-f]+$ && ! $IMAGE_TAG =~ ^(scaling-){0,1}v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid image tag. Please specify 'latest', a valid SHA1 hash, or a version tag."
    exit 1
fi

if [[ $IMAGE_TAG == "latest" ]]; then
    # Extract the git commit hash by querying the ECR repository with this horrible command...
    IMAGE_TAG=$(aws ecr describe-images --repository-name ubsv --region eu-north-1 | jq -r '.imageDetails[] | select(.imageTags // [] | any(. == "latest-arm64")) | .imageTags | map(select(. != "latest-arm64")) | .[0]' | sed 's/-arm64//')
fi

IMAGE_NAME="434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:$IMAGE_TAG-arm64"

echo "Using image tag: $IMAGE_TAG" >&2

REGION=$(kubectl config view --minify --output 'jsonpath={..name}' | cut -d ' ' -f1 | cut -d ':' -f 4)
NAMESPACE=$(kubectl config view --minify --output 'jsonpath={..namespace}')

case $REGION in
    "eu-west-1")
        LEGAL_NAMESPACES=("miner-eu-1" "m1")
        LEGAL_ENVIRONMENT=("allinone" "scaling")
        ;;

    "us-east-1")
        LEGAL_NAMESPACES=("miner-us-1" "m2")
        LEGAL_ENVIRONMENT=("allinone" "scaling")
        ;;

    "ap-south-1")
        LEGAL_NAMESPACES=("miner-ap-1" "m3")
        LEGAL_ENVIRONMENT=("allinone" "scaling")
        ;;

    "ap-south-1")
        LEGAL_NAMESPACES=("miner-ap-1" "m3")
        LEGAL_ENVIRONMENT=("allinone" "scaling")
        ;;

    "docker-desktop")
        LEGAL_NAMESPACES=("miner-lo-1")
        LEGAL_ENVIRONMENT=("docker-desktop")

        TRAEFIK=$(kubectl get namespaces traefik --output 'jsonpath={..name}' 2> /dev/null)
        if [[ -z $TRAEFIK ]]; then
            echo "Traefik is not installed, installing it now..." >&2
            helm repo add traefik https://traefik.github.io/charts >&2
            helm repo update >&2
            kubectl create namespace traefik >&2
            helm install --namespace traefik -f $DIR/deploy-traefik-extra.yaml traefik traefik/traefik >&2
        fi

        # docker pull $IMAGE_NAME > /dev/null
        # if [[ $? -ne 0 ]]; then
        #     echo "Failed to pull image: $IMAGE_NAME" >&2
        #     echo "Login to ECR and try again." >&2
        #     # Force authentication to ECR...
        #     echo "aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com" >&2
        #     exit 1
        # fi

        # Create the secret for local k8s to pull from ECR
        kubectl delete secret ecr-secret >&2
        kubectl create namespace $NAMESPACE >&2
        kubectl create secret docker-registry ecr-secret --docker-server=434394763103.dkr.ecr.eu-north-1.amazonaws.com --docker-username=AWS --docker-password=$(aws ecr get-login-password --region eu-north-1) >&2

        ;;
esac

# Check if the current namespace is in the list of legal namespaces
if [[ " ${LEGAL_NAMESPACES[@]} " =~ " ${NAMESPACE} " ]]; then
    echo "Namespace is legal" >&2
else
    echo "NAMESPACE mismatch. You are in $REGION region with a namespace of $NAMESPACE." >&2
    exit 1
fi

# Check if the current namespace is in the list of legal namespaces
if [[ " ${LEGAL_ENVIRONMENT[@]} " =~ " ${ENVIRONMENT} " ]]; then
    echo "Environment is legal" >&2
else
    echo "ENVIRONMENT mismatch. You are in $REGION region with an environment of $ENVIRONMENT." >&2
    exit 1
fi


# Check the folders exist...
if [[ ! -d $DIR/k8s/base/${ENVIRONMENT}-miner ]]; then
    echo ""
    echo "Configuration mismatch.  It is not possible to process region $REGION in $ENVIRONMENT with the $NAMESPACE namespace"
    echo "The folder $DIR/k8s/base/${ENVIRONMENT}-miner does not exist."
    echo ""
    exit 1
fi

if [[ ! -d $DIR/k8s/$REGION/$ENVIRONMENT ]]; then
    echo ""
    echo "Configuration mismatch.  It is not possible to process region $REGION in $ENVIRONMENT with the $NAMESPACE namespace"
    echo "The folder $DIR/k8s/$REGION/$ENVIRONMENT does not exist."
    echo ""
    exit 1
fi

cd $DIR/k8s/base/${ENVIRONMENT}-miner


if [[ -n $FILTER ]]; then
    mv kustomization.yaml kustomization.yaml.bak
    FILTER="$FILTER|config-map"

    in_resources=0

    while IFS= read -r line; do
    if [[ "$line" == "resources:"* ]]; then
        # We are entering the resources section
        in_resources=1
        echo "$line" >> kustomization.yaml
    elif [[ $in_resources -eq 1 && "$line" == "  - "* ]]; then
        # We are in the resources section, check if the line matches the regex pattern
        if echo "$line" | grep -qE "$FILTER"; then
            echo "$line" >> kustomization.yaml
        fi
    else
        # We are not in the resources section or have exited it
        if [[ $in_resources -eq 1 && "$line" != "  - "* ]]; then
            in_resources=0
        fi
        echo "$line" >> kustomization.yaml
    fi
    done < kustomization.yaml.bak
else
    cp kustomization.yaml kustomization.yaml.bak
fi

kustomize edit set image REPO:IMAGE:TAG=$IMAGE_NAME

cd $DIR/k8s/$REGION/$ENVIRONMENT

kustomize build . $@

cd $DIR/k8s/base/${ENVIRONMENT}-miner

cp kustomization.yaml.bak kustomization.yaml

cd $DIR
