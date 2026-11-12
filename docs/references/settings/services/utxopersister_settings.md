# UTXO Persister Service Settings

**Related Topic**: [UTXO Persister Service](../../../topics/services/utxoPersister.md)

## Configuration Settings

| Setting | Type | Default | Environment Variable | Usage |
|---------|------|---------|---------------------|-------|
| UTXOPersisterBufferSize | string | "256KB" | utxoPersister_buffer_size | **CRITICAL** - Buffer size for UTXO file I/O operations |
| UTXOPersisterDirect | bool | true | direct | Direct I/O operations control |

## Configuration Dependencies

### Buffer Management
- `UTXOPersisterBufferSize` is parsed using bytesize.Parse()
- Used for buffered I/O operations when reading/writing UTXO files
- The default of `256KB` applies to every consumer
- The values below are the fallbacks used only when the configured value fails to parse, and they differ by consumer rather than by direction:

    - The file storer's write buffer falls back to 128KB
    - The block persister's subtree-data read buffer falls back to 128KB
    - The utxo-additions, utxo-deletions and previous-utxo-set read buffers fall back to 4096 bytes

### I/O Operations
- `UTXOPersisterDirect` controls direct I/O behavior
- Affects how buffer operations are performed

## Service Dependencies

| Dependency | Interface | Usage |
|------------|-----------|-------|
| BlockStore | blob.Store | **CRITICAL** - UTXO set storage and retrieval |
| BlockchainClient | blockchain.ClientI | **CRITICAL** - Blockchain operations and block notifications |

## Validation Rules

| Setting | Validation | Impact | When Checked |
|---------|------------|--------|-------------|
| UTXOPersisterBufferSize | Must be valid byte size format | I/O performance | During service initialization |
| UTXOPersisterDirect | Controls I/O operation mode | Performance behavior | During service initialization |

## Configuration Examples

### Basic Configuration

```text
utxoPersister_buffer_size = "256KB"
direct = true
```

### Performance Tuning

```text
utxoPersister_buffer_size = "1MB"
direct = true
```

### Memory Constrained

```text
utxoPersister_buffer_size = "1KB"
direct = false
```
