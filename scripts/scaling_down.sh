#!/bin/bash

CONTEXT=$(kubectl config current-context)

kubectl config use-context arn:aws:eks:eu-north-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n m1 tx-blaster1 --replicas 0
kubectl scale deployment -n m1 coinbase1 --replicas 0
kubectl scale deployment -n m1 miner1 --replicas 0
kubectl scale deployment -n m1 propagation1 --replicas 0
kubectl scale deployment -n m1 validation1 --replicas 0
kubectl scale deployment -n m1 blockvalidation1 --replicas 0
kubectl scale deployment -n m1 blob1 --replicas 0
kubectl scale deployment -n m1 blockassembly1 --replicas 0
kubectl scale deployment -n m1 blockchain1 --replicas 0

# Return to the starting context
kubectl config use-context $CONTEXT
