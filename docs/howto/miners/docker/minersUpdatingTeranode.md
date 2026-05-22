# Update Docker Teranode

Last modified: 28-April-2026

Docker updates are managed from the
[teranode-quickstart](https://github.com/bsv-blockchain/teranode-quickstart)
repository. Quickstart keeps the local Teranode image tag in `.env` as
`TERANODE_VERSION`.

## Check for an Update

```bash
./update.sh --check
```

The script compares your local `TERANODE_VERSION` with the latest Teranode
GitHub release and prints the current tag, target tag, and release URL.

## Apply an Update

```bash
./update.sh
./start.sh
./status.sh
```

`update.sh` only rewrites `TERANODE_VERSION` in `.env`. It does not stop Docker,
pull images, or recreate containers. Running `./start.sh` after the version
change pulls the selected image and recreates containers whose image tag changed.

For non-interactive use:

```bash
./update.sh --yes
./start.sh
```

## Pin or Roll Back

Set a specific Teranode release tag:

```bash
./update.sh --to v0.14.2
./start.sh
```

Use this for rollback only after checking the release notes for data-format or
migration warnings.

## Update Quickstart Itself

`./update.sh` updates the Teranode version pin, not the quickstart repository.
To update quickstart scripts and Compose files:

```bash
git pull
```

Then review `.env.example` for new settings that may be relevant to your local
`.env`.

## Operational Notes

- Data volumes are preserved during normal updates.
- Docker Compose updates are not rolling updates; expect downtime while changed
  containers restart.
- If a release requires data migration or a full resync, the Teranode release
  notes should call that out.
- Keep a copy of `.env` and any external reverse-proxy configuration before
  major changes.

## Related Documentation

- Quickstart update details: [`docs/UPDATING.md`](https://github.com/bsv-blockchain/teranode-quickstart/blob/main/docs/UPDATING.md)
- Teranode releases: <https://github.com/bsv-blockchain/teranode/releases>
- [Start and Stop Docker Teranode](./minersHowToStopStartDockerTeranode.md)
