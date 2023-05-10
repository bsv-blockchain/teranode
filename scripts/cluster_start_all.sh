#!/bin/bash

kubectl scale deployment -n seeder-service --replicas 1 --all

kubectl scale deployment -n utxostore-service --replicas 1 --all

kubectl scale deployment -n validation-service --replicas 4 --all

echo "Sleeping for 30 seconds to allow all validation services to spin up"
sleep 30
echo "Starting propagation services"

kubectl scale deployment -n propagation-service --replicas 4 --all
sleep 30


kubectl scale deployment -n tx-blaster-service --replicas 4 --all

kubectl get pods --all-namespaces -o wide | grep "-service"
