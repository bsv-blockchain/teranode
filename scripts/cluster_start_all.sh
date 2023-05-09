#!/bin/bash

kubectl scale deployment -n seeder-service --replicas 1 --all

kubectl scale deployment -n utxostore-service --replicas 1 --all

kubectl scale deployment -n validation-service --replicas 5 --all
sleep 4

kubectl scale deployment -n propagation-service --replicas 2 --all
sleep 4


kubectl scale deployment -n tx-blaster-service --replicas 2 --all

kubectl get pods --all-namespaces -o wide | grep "-service"
