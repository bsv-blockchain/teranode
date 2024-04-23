# For anyone with an apple silicon macbook pro and want to test the fan…

## node setup
Ensure node version is greater than 20.

## docker setup

I give docker 32GB RAM and 128GB disk and 10 CPU (not sure of the minimum requirement yet)
If memory is too much then try reducing miner_waitSeconds value (docker default is 5 mins)

The docker-compose.yml file is the default compse file and assumes you have lots of RAM. If you have a 16GB machine then use the docker-compose-12g.yml which limits the whole setup to 12GB RAM.

## build docker images
```
$ docker compose build
```

(you may need to login)

```
$ CERT=$(aws ecr get-login-password --region eu-north-1)
echo $CERT | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com >&2
```

## run x3 teranodes with miners, coinbase, and tx-blasters
```
$ docker compose up -d
```
It will mine the initial blocks, generate splitting coinbase txs and run 3 blasters.


## to see what containers are running with memory usage, etc
```
$ docker stats
```

## run with a subset of services (this example sparks up a single node)
```
$ docker compose up ubsv-1 -d
```


## To see whether they are in sync:

```
$ ./scripts/bestblock-docker.sh
```

Occasionally, ubsv-1, ubsv-2 or ubsv-3 fail to start because aerospike/postgres wasn’t ready at that moment. Just run the ‘up’ command again. (If anyone knows how to make ubsv containers wait for their dependent services to be ‘ready’…)

## To delete everything
If you are using the default compose then postgres is used to store txmeta and utxo. These are persisted in data/postgres
```
$ rm -rf data/postgres
$ docker compose down
```

(This can take a minute or two to complete)

## Note, if you need to run tx-baster separately on a cmd shell use
```
$ cd cmd/txblaster
$ logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv1 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false
$ logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv2 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false
$ logLevel=INFO SETTINGS_CONTEXT=docker.ci.externaltxblaster.ubsv3 go run . -workers=1000 -print=0 -profile=:9092 -log=0  -quic=false
```

## To override the base compose to use Aerospike services
```
$ docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml up
$ docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml down
```

## To override the base compose to use dedicated kafka services for each node
```
$ docker compose -f docker-compose.yml -f docker-compose.kafka.override.yml up
$ docker compose -f docker-compose.yml -f docker-compose.kafka.override.yml down
```

## To override the base compose to use more than 1 tx-blaster
```
$ docker compose -f docker-compose.yml -f docker-compose.txBlaster.override.yml up
$ docker compose -f docker-compose.yml -f docker-compose.txBlaster.override.yml down
```

## To override the base compose to run on a 16GB macbook with 12GB allocated to Docker
```
$ docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml -f docker-compose.12gb.override.yml up
$ docker compose -f docker-compose.yml -f docker-compose.aerospike.override.yml -f docker-compose.12gb.override.yml down
```

## To tail the log files of everything
```
$ docker compose logs
```

## To tail the log files of a specific service
```
$ docker compose logs ubsv-1
```

## URLs for viewing dashboards

Most of the ports you are familiair with are mapped to unique ports spanning all 3 nodes.
Prepend the port number with 1, 2 or 3 to specify which node you need.

usbv-1 http://localhost:18090

usbv-2 http://localhost:28090

usbv-3 http://localhost:38090

## grafana

http://localhost:3000
(The very first time you use this the login is admin/admin)