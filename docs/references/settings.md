# ⚙️Settings

All services accept settings through a centralized Settings object that allows local and remote servers to have their own specific configuration.

Please review the following documents for more information on how to deploy the settings:

- [Developer Setup](../tutorials/developers/developerSetup.md)
- [Test Setup](../howto/miners/docker/minersHowToConfigureTheNode.md)
- [Production Setup](../howto/miners/kubernetes/minersHowToConfigureTheNode.md)

## Configuration Files

The settings are stored in 2 files:

- `settings.conf` - Global settings with sensible defaults for all environments (local/dev/operator)
- `settings_local.conf` - Developer-specific and deployment-specific settings, local overrides (not tracked in source control)

When developing locally, you should:

1. Use `settings.conf` for default configuration values, and not modify it unless intended as a global change to be shared with other developers and operators.
2. Use `settings_local.conf` for your personal development settings and overrides

## Configuration System

The configuration system allows for a layered approach to settings. At its core, it works with a base setting. However, to cater to individualized or context-specific requirements, you can have context-dependent overrides.

Here's how it prioritizes:

1. `SETTING_NAME.context_name`: A context-specific override (highest priority)
2. `SETTING_NAME.base`: A general override
3. `SETTING_NAME`: The base setting (lowest priority)

### Example

Suppose we have a base setting named `DATABASE_URL` to define the database connection URL for our application.

The base setting might be:

```text
DATABASE_URL = "database-url-default.com"
```

As an example, we might have a `newenvironment1` database for development purposes. So, for the context `dev.newenvironment1`, there might be an override:

```text
DATABASE_URL.dev.newenvironment1 = "database-url-environment1"
```

There might also be a generic development database URL, defined as:

```text
DATABASE_URL.dev = "database-url-dev.com"
```

When the application is run against this development context (`dev.newenvironment1`):

- The system first checks for `DATABASE_URL.dev.newenvironment1`. If it exists, it's used.
- If not, it falls back to the general development URL `DATABASE_URL.dev`.
- If neither exists, it defaults to `DATABASE_URL`.

For `newenvironment1`, the resolution would be:

1. **First Preference:** `DATABASE_URL.dev.newenvironment1` -> "database-url-dev.com"
2. **Fallback:** `DATABASE_URL.dev` -> "database-url-dev.com"
3. **Last Resort:** `DATABASE_URL` -> "database-url-default.com"

This approach provides flexibility to have a default setting, an optional general override, and further context-specific overrides. It's a hierarchical system that allows fine-grained control over configurations based on context.

## Accessing Settings in Go

The settings are accessed through a centralized Settings object that is passed to services requiring configuration. Here's how to use it:

### Initialization

First, create a new Settings instance:

```go
settings := settings.NewSettings()
```

This will load all configuration values from the settings files according to the priority system described above.

### Accessing Settings

Settings are organized into logical groups within the Settings struct. For example:

```go
// Access Kafka settings
kafkaHosts := settings.Kafka.Hosts
kafkaPort := settings.Kafka.Port

// Access Blockchain settings
grpcAddress := settings.BlockChain.GRPCAddress
maxRetries := settings.BlockChain.MaxRetries

// Access Alert settings
genesisKeys := settings.Alert.GenesisKeys
p2pPort := settings.Alert.P2PPort
```

### Available Setting Groups

The Settings struct includes multiple setting groups:

- Alert Settings (AlertSettings)
- Asset Settings (AssetSettings)
- Block Settings (BlockSettings)
- BlockChain Settings (BlockChainSettings)
- BlockValidation Settings (BlockValidationSettings)
- Kafka Settings (KafkaSettings)
- Validator Settings (ValidatorSettings)
- And more...

Each group contains related configuration values specific to that component of the system.

### Best Practices

1. Always pass the Settings object as a dependency to services that need configuration:

    ```go
    func NewService(logger ulogger.Logger, settings *settings.Settings) *Service {
        return &Service{
            logger: logger,
            settings: settings,
            // ...
        }
    }
    ```

2. Access settings through the appropriate group rather than using direct key access:

    ```go
    // Good
    maxRetries := settings.BlockChain.MaxRetries

    // Avoid (historical style, now deprecated)
    maxRetries, _ := gocore.Config().GetInt("blockchain_maxRetries")
    ```

3. Use the type system to your advantage - settings are strongly typed within their respective groups.

**Note**: The old `gocore.Config()` approach with direct key access is deprecated. Always use the new Settings object for accessing configuration values.

## Detailed Settings Reference

For comprehensive documentation of all available settings, see the following references organized by component:

### Global Settings

- [Global Settings](settings/global_settings.md) - System-wide settings (tracing, logging, security, gRPC, monitoring)
- [Policy Settings](settings/policy_settings.md) - BSV Blockchain consensus rules and transaction validation policies

### Services

- [Alert Service Settings](settings/services/alert_settings.md)
- [Asset Server Settings](settings/services/asset_settings.md)
- [Block Assembly Settings](settings/services/blockassembly_settings.md)
- [Blockchain Settings](settings/services/blockchain_settings.md)
- [Block Persister Settings](settings/services/blockpersister_settings.md)
- [Block Validation Settings](settings/services/blockvalidation_settings.md)
- [Legacy Settings](settings/services/legacy_settings.md)
- [P2P Settings](settings/services/p2p_settings.md)
- [Propagation Settings](settings/services/propagation_settings.md)
- [RPC Settings](settings/services/rpc_settings.md)
- [Subtree Validation Settings](settings/services/subtreevalidation_settings.md)
- [UTXO Persister Settings](settings/services/utxopersister_settings.md)
- [Validator Settings](settings/services/validator_settings.md)

### Stores

- [Aerospike Store Settings](settings/stores/aerospike_settings.md)
- [Blob Store Settings](settings/stores/blob_settings.md)
- [UTXO Store Settings](settings/stores/utxo_settings.md)

### Messaging

- [Kafka Settings](settings/kafka_settings.md)
