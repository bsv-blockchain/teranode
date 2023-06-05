# UBSV
> Unbounded Bitcoin Satoshi Vision

Do the port numbers of the server also change? If not, it'd be helpful if you could provide me with the logs produced when run with the following environment variables set: GRPC_VERBOSITY=debug GRPC_TRACE=client_channel,round_robin

## Makefile
To make the proto files run

```make gen```

## Running locally in development

To run all the services in 1 terminal window, run the following command:

If you use badger as the datastore, you need to delete the data directory before running.

```shell
rm -rf data && go run -tags native . -propagation=1 -utxostore=1 -validator=1 -seeder=1 
```

If you need support for Aerospike, you need to add "aerospike" to the tags:

```shell
rm -rf data && go run -tags native,aerospike . -propagation=1 -utxostore=1 -validator=1 -seeder=1 
```

If you need support for FoundationDB, you need to add "foundationdb" to the tags:

```shell
rm -rf data && go run -tags native,foundationdb . -propagation=1 -utxostore=1 -validator=1 -seeder=1 
```
