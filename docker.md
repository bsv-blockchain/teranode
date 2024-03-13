# For anyone with an apple silicon macbook pro and want to test the fan…

## node setup
Ensure node version is greater than 20.

## docker setup

I give docker 32GB RAM and 128GB disk and 10 CPU (not sure of the minimum requirement yet)
If memory is too much then try reducing miner_waitSeconds value (docker default is 5 mins)

## build docker images
`$ docker compose build`

## run x3 teranodes with miners, coinbase, and tx-blasters
`$ docker compose up -d`
It will mine the initial blocks, generate splitting coinbase txs and run 3 blasters.
or
## run with a subset of services
`$ docker compose up postgres ubsv-1 ubsv-2 ubsv-3 -d`


## To see whether they are in sync:

`scripts$ ./bestblock-docker.sh`

Occasionally, ubsv-1, ubsv-2 or ubsv-3 fail to start because aerospike/postgres wasn’t ready at that moment. Just run the ‘up’ command again. (If anyone knows how to make ubsv containers wait for their dependent services to be ‘ready’…)

## To delete everything
`$ docker compose down`

(This can take a minute or two to complete)

## Note, if you need to run tx-baster separately on a cmd shell use
` cd cmd/txblaster`
`logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv1 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false`
`logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv2 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false`
`logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv3 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false`
