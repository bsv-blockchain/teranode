#!/bin/bash

kubectl scale deployment -n tx-blaster-service --replicas 0 --all
kubectl scale deployment -n propagation-service --replicas 0 --all
kubectl scale deployment -n validation-service --replicas 0 --all
kubectl scale deployment -n utxostore-service --replicas 0 --all
