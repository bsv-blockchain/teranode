# UBSV Docker Setup Guide

This guide will walk you through the steps to set up and run ubsv Docker containers for our project.

## Prerequisites

Before you begin, make sure you have the following prerequisites installed on your system:
- Docker
- Docker Compose

## Step 1: Start p2p-bootstrap service

If you are running for the first time or have made changes to the `modules/p2pBootstrap/Dockerfile`, use the `--build` option. Otherwise, you can skip it.

```bash
docker-compose -f docker-compose-ci-template.yml up p2p-bootstrap-1
```

## Step 2: Start ubsv-1 with miner and ubsv-2 with no miner

If you are running for the first time or have made changes to the `local.Dockerfile`, use the --build option. Otherwise, you can skip it.
Before building a new image use `export GITHUB_SHA=<GITHUB_SHA to test>`
```bash
docker-compose -f docker-compose-ci-template.yml up postgres ubsv-1 ubsv-2

curl http://localhost:18091/lastblocks?n=1, http://localhost:18091/lastblocks?n=1 and get the latest block from `height` field
Check if both nodes are in sync and have 300 blocks each
```

## Step 3: Start ubsv-1-coinbase and ubsv-2-coinbase

Start the coinbase services for both nodes. This time, you can start ubsv-2-coinbase with the miner service if required.

```bash
docker-compose -f docker-compose-ci-template.yml up ubsv-1-coinbase ubsv-2-coinbase

curl http://localhost:18091/lastblocks?n=1, http://localhost:18091/lastblocks?n=1 and get the latest block from `height` field
Check if both nodes are in sync and have 310 blocks each
```

## Step 3: Start TxBlaster

Start TX blaster

If you are running for the first time or have made changes to the `local.txblaster.Dockerfile`, use the --build option. You can start both tx-blaster-1 and tx-blaster-2 as follows:
Before building a new image use `export GITHUB_SHA=<GITHUB_SHA to test>`

```bash
docker-compose -f docker-compose-ci-template.yml up tx-blaster-1
docker-compose -f docker-compose-ci-template.yml up tx-blaster-2
```

Explore the blocks at `http://localhost:18090/viewer/`, `http://localhost:28090/viewer/` etc
