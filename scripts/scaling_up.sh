#!/bin/bash

wait() {
  local namespace=$1
  local deployment_name=$2
  local timeout=$3

  kubectl -n "$namespace" wait --for=condition=available --timeout="${timeout}s" deployment/"$deployment_name"
}


CONTEXT=$(kubectl config current-context)

kubectl config use-context arn:aws:eks:eu-north-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n m1 blockchain1 --replicas 1
wait m1 blockchain1 30

kubectl scale deployment -n m1 blob1 --replicas 1
wait m1 blob1 30

kubectl scale deployment -n m1 blockassembly1 --replicas 1
wait m1 blockassembly1 30

kubectl scale deployment -n m1 blockvalidation1 --replicas 1
wait m1 blockvalidation1 30

kubectl scale deployment -n m1 miner1 --replicas 1
wait m1 miner1 30

#kubectl scale deployment -n m1 validation1 --replicas 1
kubectl scale deployment -n m1 propagation1 --replicas 1
wait m1 propagation1 30

kubectl scale deployment -n m1 coinbase1 --replicas 1
wait m1 coinbase1 30
#kubectl scale deployment -n m1 tx-blaster1 --replicas 1

# Return to the starting context
kubectl config use-context $CONTEXT
