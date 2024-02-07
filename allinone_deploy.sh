#!/bin/bash

# Check if yq is installed
if ! command -v yq &> /dev/null
then
    echo "yq could not be found, please install it to continue (brew install yq)."
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR/deploy/k8s/base/allinone-miner

DEPLOY_DIR=$DIR/deploy/k8s/eu-west-1/allinone
CONTEXT=$(kubectl config current-context)
if [ "$CONTEXT" == "docker-desktop" ]; then
  aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com > /dev/null 2>&1
  DEPLOY_DIR=$DIR/deploy/k8s/local/allinone
fi

# kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:$(git rev-parse HEAD)
kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:ae32fd77d3f2f747c0787fd04b2ec7931afd32c7


cd $DEPLOY_DIR

kustomize build . $@

cd $DIR/deploy/k8s/base/allinone-miner

yq eval 'del(.images)' -i kustomization.yaml

cd $DIR
