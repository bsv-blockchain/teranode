#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd $DIR/deploy/k8s/base/allinone-miner

#kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:$(git rev-parse HEAD)
kustomize edit set image REPO:IMAGE:TAG=434394763103.dkr.ecr.eu-north-1.amazonaws.com/ubsv:ae32fd77d3f2f747c0787fd04b2ec7931afd32c7

cd $DIR/deploy/k8s/eu-west-1/allinone

kustomize build . $@

cd $DIR/deploy/k8s/base/allinone-miner

yq eval 'del(.images)' -i kustomization.yaml

cd $DIR

