# Chain Integrity Checker
> A tool to check the integrity of a TERANODE local blockchain

This tool will check the integrity of a local blockchain generated through testing. It is not intended to be used on
mainnet.

## Usage

```shell
# remove the old data
rm -rf data

# run the node for least 110 blocks
docker compose -f compose/docker-compose-host.yml up -d

# stop the node after mining 110+ blocks

docker compose -f compose/docker-compose-host.yml down teranode-1 teranode-2 teranode-3

# run chainintegrity
go run compose/cmd/chainintegrity/main.go --logfile=chainintegrity --debug

# cleanup
docker compose -f compose/docker-compose-host.yml down
```

