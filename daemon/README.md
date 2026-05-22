# daemon

The `daemon` package serves as the core entry point for the Teranode system. It is responsible for initializing, managing, and orchestrating various services and components required for the operation of the Teranode blockchain infrastructure.

## Usage

This package is typically used to start the Teranode daemon, which orchestrates the interaction between blockchain, networking, and other subsystems.

### Features
- **Service Management**: Handles the lifecycle of services such as blockchain, P2P networking, validation, and more.
- **Kafka Integration**: Provides utilities for creating Kafka producers and consumer groups for various subsystems.
- **Health Monitoring**: Includes health check endpoints for readiness and liveness.
- **Tracing and Metrics**: Supports OpenTelemetry and Prometheus metrics for monitoring and debugging.

## Development

- See `daemon.go` for the main logic and entry points.
- Run tests with `go test -race -tags testtxmetacache ./...` in this directory, or use `make test` from the project root.

---

For more information, see the main project documentation.
