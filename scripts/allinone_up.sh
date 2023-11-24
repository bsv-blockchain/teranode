#!/bin/bash

CONTEXT=$(kubectl config current-context)

kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner1 miner1 --replicas 1
kubectl scale deployment -n miner2 miner2 --replicas 1
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner4 miner4 --replicas 1
kubectl scale deployment -n miner5 miner5 --replicas 1
kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner7 miner7 --replicas 1
kubectl scale deployment -n miner8 miner8 --replicas 1

# Return to the starting context
kubectl config use-context $CONTEXT

#kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
#kubectl scale deployment -n miner1 tx-blaster1 --replicas 1
#kubectl scale deployment -n miner2 tx-blaster2 --replicas 1
#kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
#kubectl scale deployment -n miner4 tx-blaster4 --replicas 1
#kubectl scale deployment -n miner5 tx-blaster5 --replicas 1
#kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
#kubectl scale deployment -n miner7 tx-blaster7 --replicas 1
#kubectl scale deployment -n miner8 tx-blaster8 --replicas 1
