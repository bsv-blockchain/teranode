# Test CPU Miner

Teranode provides a basic CPU miner that can be used for testing purposes. When using the Docker setup described here you need to make sure to run it as part of your defined Docker network. For Kubernetes and other deployments, adjust accordingly.

```bash
docker run -it \
--entrypoint="" --network my-teranode-network \
434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 \
/app/miner.run -rpcconnect rpc -rpcuser bitcoin -rpcpassword bitcoin -rpcport 9292 \
-coinbase-addr mgqipciCS56nCYSjB1vTcDGskN82yxfo1G -coinbase-sig "/Your miner tag/" \
-cpus 2
```

The example address used here is a testnet address, linked to the [BSV Faucet](https://bsvfaucet.org/).
