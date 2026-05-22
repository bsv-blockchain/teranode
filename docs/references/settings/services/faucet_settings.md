# Faucet Service Settings

The Faucet is a minimal HTTP service that dispenses test Bitcoin for development and integration testing. It has no authentication by design and must never be enabled on mainnet.

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| HTTPListenAddress | string | "" | faucet_httpListenAddress | HTTP listen address for the faucet service; empty = disabled |

## Configuration Details

### HTTPListenAddress

Controls whether the faucet service starts and which address it binds to.

- **Empty string** (default): Faucet is disabled.
- **`:port`** format: Listens on all network interfaces (e.g., `:8091`).
- **`host:port`** format: Binds to a specific interface (e.g., `localhost:8091`).

## Valid Environments

Only enable the faucet in non-production environments:

- Local development (regtest)
- Testnet deployments
- Integration test networks

**Never enable on mainnet.**

## Security Considerations

- No authentication built in — open access by design for test networks.
- No rate limiting built in — add a reverse proxy if needed.
- Restrict access with firewall rules when exposed on shared test networks.

## Configuration Examples

### Enable for Local Development

```bash
faucet_httpListenAddress=:8091
```

### Enable on a Specific Interface Only

```bash
faucet_httpListenAddress=127.0.0.1:8091
```

### Disable (default)

```bash
faucet_httpListenAddress=
```
