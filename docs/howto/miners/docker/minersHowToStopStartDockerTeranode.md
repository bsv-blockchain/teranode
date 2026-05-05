# Start and Stop Docker Teranode

Last modified: 28-April-2026

Use the scripts in
[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
instead of invoking Docker Compose directly. The scripts load `.env`, use the
right Compose profiles, handle FSM transitions, and keep common commands
consistent.

## Start

From the quickstart repository root:

```bash
./start.sh
```

`start.sh` runs `docker compose up -d`, then performs the Teranode FSM startup
transition. It always prints the local URLs for the asset viewer and RPC. When
the `monitoring` profile is enabled, it also prints Grafana, Prometheus, and
Kafka Console URLs.

Check service state:

```bash
./status.sh
```

The status script shows:

- Docker Compose service status
- Teranode FSM state
- Chain information through RPC

## Logs

Tail all logs:

```bash
./logs.sh
```

Tail a specific service:

```bash
./logs.sh blockchain
./logs.sh asset
./logs.sh p2p
```

Use the service names from `docker compose ps` when you need a different
container.

## RPC and CLI Checks

Quickstart wraps common calls so credentials and container names do not need to
be repeated:

```bash
./rpc.sh getblockchaininfo
./rpc.sh getblockcount
./cli.sh getfsmstate
```

If the FSM startup transition races during boot, set it manually:

```bash
./cli.sh setfsmstate --fsmstate RUNNING
```

## Stop

Use the stop script for normal shutdown:

```bash
./stop.sh
```

`stop.sh` asks Teranode to enter `IDLE` state, then runs `docker compose down`.
Data volumes are preserved.

## Restart

For a normal restart:

```bash
./stop.sh
./start.sh
./status.sh
```

## Direct Docker Compose Use

Direct `docker compose` commands are still useful for advanced inspection:

```bash
docker compose ps
docker compose logs -f blockchain
docker compose config
```

Run them from the quickstart repository root so Compose reads the correct
`docker-compose.yml` and `.env`.

## Related Documentation

- [Install Teranode with Docker](./minersHowToInstallation.md)
- [Configure Docker Teranode](./minersHowToConfigureTheNode.md)
- [Troubleshooting Docker Teranode](./minersHowToTroubleshooting.md)
