#!/bin/bash

CONTEXT=$(kubectl config current-context)

kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner1 miner1 --replicas 0
kubectl scale deployment -n miner2 miner2 --replicas 0
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner4 miner4 --replicas 0
kubectl scale deployment -n miner5 miner5 --replicas 0
kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner7 miner7 --replicas 0
kubectl scale deployment -n miner8 miner8 --replicas 0

# Return to the starting context
kubectl config use-context $CONTEXT
