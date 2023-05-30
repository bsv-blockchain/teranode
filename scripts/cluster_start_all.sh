#!/bin/bash

kubectl scale deployment -n seeder-service --replicas 1 --all

kubectl scale deployment -n utxostore-service --replicas 1 --all

kubectl scale deployment -n blockassembly-service --replicas 5 --all

kubectl scale deployment -n validation-service --replicas 5 --all

echo "Sleeping for 30 seconds to allow all validation services to spin up"
sleep 30
echo "Starting propagation services"

kubectl scale deployment -n propagation-service --replicas 2 --all
sleep 30


kubectl scale deployment -n tx-blaster-service --replicas 2 --all
echo "Sleeping for 30 seconds to allow all propagation services to spin up"
sleep 30
echo "Starting propagation services"

kubectl get pods --all-namespaces | grep "-service"
