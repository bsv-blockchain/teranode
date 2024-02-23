# For anyone with an apple silicon macbook pro and want to test the fan…

## docker setup

I give docker 32GB RAM and 128GB disk and 10 CPU (not sure of the minimum requirement yet)
If memory is too much then try reducing miner_waitSeconds value (docker default is 5 mins)

## build docker images
`$ docker compose -f docker-compose-ci-ext-p2p.yml build`

## run x3 teranodes with miners, coinbase, and tx-blasters
`$ docker compose -f docker-compose-ci-ext-p2p.yml up -d`

It will mine the initial blocks, generate splitting coinbase txs and run 3 blasters.

## To see whether they are in sync:

`scripts$ ./bestblock-docker.sh`

Occasionally, ubsv-1, ubsv-2 or ubsv-3 fail to start because aerospike/postgres wasn’t ready at that moment. Just run the ‘up’ command again. (If anyone knows how to make ubsv containers wait for their dependent services to be ‘ready’…)

## To delete everything
`$ docker compose f docker-compose-ci-ext-p2p.yml down`

(This can take a minute or two to complete)