# order is important here, do not change unless you know what you're doing
kubectl scale deployment -n blockchain-service blockchain --replicas 1
kubectl scale deployment -n blob-service blob --replicas 1
kubectl scale deployment -n coinbase-service coinbase --replicas 1
kubectl scale deployment -n blockassembly-service blockassembly --replicas 1
kubectl scale deployment -n validation-service validation --replicas 1
kubectl scale deployment -n propagation-service propagation --replicas 1
kubectl scale deployment -n miner1 miner1 --replicas 1
kubectl scale deployment -n miner2 miner2 --replicas 1
kubectl scale deployment -n miner3 miner3 --replicas 1
