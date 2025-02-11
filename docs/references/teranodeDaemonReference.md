# Daemon Reference

The `daemon` package provides core functionality for initializing and managing Teranode services. It handles service lifecycle management, configuration, and coordination between different components of the system.

## Overview

The daemon is responsible for:
- Starting and stopping Teranode services
- Managing service dependencies and initialization order
- Handling configuration and settings
- Coordinating between different components
- Managing stores (UTXO, transaction, block, etc.)
- Health check endpoints

Besides being used for starting the Teranode services in our main application, the daemon package can also be used in tests to run Teranode instances with different configurations.

## Core Components

### Daemon Structure

```go
type Daemon struct {
    doneCh chan struct{}
}
```

The Daemon struct contains a `doneCh` channel for shutdown coordination.

### Main Functions

#### New()

Creates a new Daemon instance:

```go
func New() *Daemon {
    return &Daemon{
        doneCh: make(chan struct{}),
    }
}
```

#### Start(logger, args, settings, readyCh)

Starts the daemon and initializes services based on configuration:

- `logger`: Logger instance for output
- `args`: Command line arguments for service selection
- `settings`: Configuration settings
- `readyCh`: Optional channel to signal when initialization is complete

#### Stop()

Gracefully shuts down the daemon and all running services.

## Service Management

### Service Initialization

Services are initialized based on command-line flags or configuration settings. Each service can be enabled/disabled using:

- Command line: `-servicename=1` or `-servicename=0`
- Global disable: `-all=0` disables all services unless explicitly enabled

### Available Services

- Alert
- Asset
- Blockchain
- BlockAssembly
- BlockPersister
- BlockValidation
- Legacy
- P2P
- Propagation
- RPC
- SubtreeValidation
- UTXOPersister
- Validator

### Health Checks

The daemon provides HTTP endpoints for health monitoring:

- `/health/readiness`: Readiness check
- `/health/liveness`: Liveness check
- Port configurable via `HealthCheckPort` setting

## Store Management

The daemon manages several types of stores:

### Transaction Store
```go
func GetTxStore(logger ulogger.Logger) (blob.Store, error)
```

### UTXO Store
```go
func GetUtxoStore(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings) (utxostore.Store, error)
```

### Block Store
```go
func GetBlockStore(logger ulogger.Logger) (blob.Store, error)
```

### Subtree Store
```go
func GetSubtreeStore(logger ulogger.Logger, tSettings *settings.Settings) (blob.Store, error)
```

All stores follow a singleton pattern, ensuring only one instance exists per store type.

## Testing Support

The daemon package is designed to support comprehensive testing scenarios:

- Can be initialized with different store implementations (SQLite, in-memory, etc.)
- Supports running complete Teranode instances in tests
- Allows step-by-step debugging of services
- Facilitates testing complex scenarios like double-spend detection

Example test initialization:

```go

func NewDoubleSpendTester(t *testing.T) *DoubleSpendTester {
    ctx, cancel := context.WithCancel(context.Background())

    logger := ulogger.NewErrorTestLogger(t, cancel)

    // Delete the sqlite db at the beginning of the test
    _ = os.RemoveAll("data")

    persistentStore, err := url.Parse("sqlite:///test")
    require.NoError(t, err)

    memoryStore, err := url.Parse("memory:///")
    require.NoError(t, err)

    if !isKafkaRunning() {
        kafkaContainer, err := testkafka.RunTestContainer(ctx)
        require.NoError(t, err)

        t.Cleanup(func() {
            _ = kafkaContainer.CleanUp()
        })

        gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))
    }

    tSettings := settings.NewSettings() // This reads gocore.Config and applies sensible defaults

    // Override with test settings...
    tSettings.SubtreeValidation.SubtreeStore = memoryStore
    tSettings.BlockChain.StoreURL = persistentStore
    tSettings.UtxoStore.UtxoStore = persistentStore
    tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
    tSettings.Asset.CentrifugeDisable = true

    readyCh := make(chan struct{})

    d := daemon.New()

    go d.Start(logger, []string{
        "-all=0",
        "-blockchain=1",
        "-subtreevalidation=1",
        "-blockvalidation=1",
        "-blockassembly=1",
        "-asset=1",
        "-propagation=1",
    }, tSettings, readyCh)

    <-readyCh

    bcClient, err := blockchain.NewClient(ctx, logger, tSettings, "test")
    require.NoError(t, err)

    baClient, err := blockassembly.NewClient(ctx, logger, tSettings)
    require.NoError(t, err)

    propagationClient, err := propagation.NewClient(ctx, logger, tSettings)
    require.NoError(t, err)

    blockValidationClient, err := blockvalidation.NewClient(ctx, logger, tSettings, "test")
    require.NoError(t, err)

    w, err := wif.DecodeWIF(tSettings.BlockAssembly.MinerWalletPrivateKeys[0])
    require.NoError(t, err)

    privKey := w.PrivKey

    subtreeStore, err := daemon.GetSubtreeStore(logger, tSettings)
    require.NoError(t, err)

    utxoStore, err := daemon.GetUtxoStore(ctx, logger, tSettings)
    require.NoError(t, err)

    return &DoubleSpendTester{
        ctx:                   ctx,
        logger:                logger,
        d:                     d,
        blockchainClient:      bcClient,
        blockAssemblyClient:   baClient,
        propagationClient:     propagationClient,
        blockValidationClient: blockValidationClient,
        privKey:               privKey,
        subtreeStore:          subtreeStore,
        utxoStore:             utxoStore,
    }
}
```

Additionally, using the following function:
```
tSettings := settings.NewSettings() // This reads gocore.Config and applies sensible defaults
```
will initialise settings with generic defaults, ideal for tests.

## Configuration

The daemon uses a combination of (in this priority order):
1. Command line arguments
2. Environment variables
3. Configuration files
4. Default settings

Notice that the service does not use the
