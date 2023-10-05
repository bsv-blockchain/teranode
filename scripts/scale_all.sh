ksd tx-blaster1 --replicas 0 --all
ksd miner1 --replicas 0 --all
ksd propagation1 --replicas 0 --all
ksd coinbase1 --replicas 0 --all
ksd blockassembly1 --replicas 0 --all
ksd blob1 --replicas 0 --all
ksd blockchain1 --replicas 0 --all
ksd validation1 --replicas 0 --all
ksd txmetastore1 --replicas 0 --all
ksd utxostore1 --replicas 0 --all

ksd tx-blaster1 --replicas 1 --all
ksd propagation1 --replicas 1 --all


# Blockchain must be first (UTXOStore uses it)
ksd blockchain1 --replicas 1 --all

ksd txmetastore1 --replicas 1 --all
ksd utxostore1 --replicas 1 --all
ksd blob1 --replicas 1 --all
ksd blockassembly1 --replicas 1 --all
