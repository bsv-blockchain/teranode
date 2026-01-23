# Blob Store Settings

**Related Topic**: [Blob Store](../../../topics/stores/blob.md)

## URL Configuration Parameters

| Parameter | Type | Default | Usage | Impact |
|-----------|------|---------|-------|--------|
| batch | bool | false | `storeURL.Query().Get("batch") == "true"` | **CRITICAL** - Enables batch wrapper for performance |
| sizeInBytes | int64 | 4194304 | `storeURL.Query().Get("sizeInBytes")` | **CRITICAL** - Controls batch memory usage |
| writeKeys | bool | false | `storeURL.Query().Get("writeKeys") == "true"` | Enables key-based retrieval from batches |
| localDAHStore | string | "" | `storeURL.Query().Get("localDAHStore") != ""` | **CRITICAL** - Enables Delete-At-Height functionality |
| localDAHStorePath | string | "/tmp/localDAH" | `storeURL.Query().Get("localDAHStorePath")` | DAH metadata storage directory |
| logger | bool | false | `storeURL.Query().Get("logger") == "true"` | **CRITICAL** - Enables debug logging wrapper |
| hashPrefix | int | 0 | `storeURL.Query().Get("hashPrefix")` | **CRITICAL** - Hash-based directory structure (first N chars) |
| hashSuffix | int | 0 | `storeURL.Query().Get("hashSuffix")` | **CRITICAL** - Hash-based directory structure (last N chars) |
| checksum | bool | false | File backend parameter | **CRITICAL** - SHA256 checksumming for data integrity |
| header | string | "" | File backend parameter | Custom header prepended to blobs |

## Configuration Dependencies

### Batch Processing
- When `batch = true`, uses `sizeInBytes` for memory control
- `writeKeys` enables key indexing when batching enabled
- Creates batcher wrapper with configured size and key options

### Delete-At-Height (DAH)
- When `localDAHStore` is set, enables DAH functionality
- Uses `localDAHStorePath` for metadata storage location
- Creates DAH wrapper with file-based cache store

### Hash-based Directory Organization
- `hashPrefix` uses first N characters of hash for directories
- `hashSuffix` uses last N characters of hash for directories
- Negative hashPrefix value uses suffix behavior

### Data Integrity
- When `checksum = true`, creates .sha256 files alongside blobs
- Validates checksums during read operations
- Removes checksum files during deletion

### Debug Logging
- When `logger = true`, wraps store with logging functionality
- Logs all store operations at DEBUG level
- Enables detailed operation debugging

## Backend Support

| Backend | Scheme | Parameters Supported |
|---------|--------|---------------------|
| null | null:// | logger (localDAHStore blocked) |
| memory | memory:// | All common parameters |
| file | file:// | All parameters including checksum, header |
| http | http:// | All common parameters |
| s3 | s3:// | All common parameters |

## Validation Rules

| Parameter | Validation | Impact | When Checked |
|-----------|------------|--------|-------------|
| batch | Boolean string check | Batch wrapper creation | During store initialization |
| sizeInBytes | ParseInt validation | Batch memory allocation | During batcher creation |
| writeKeys | Boolean string check | Key indexing behavior | During batcher creation |
| localDAHStore | Non-empty string check | DAH functionality | During store initialization |
| hashPrefix | ParseInt validation | Directory structure | During file store creation |
| hashSuffix | ParseInt validation | Directory structure | During file store creation |

## Configuration Examples

### Basic File Store

```text
file:///data/store
```

### Batched Store with DAH

```text
file:///data/store?batch=true&sizeInBytes=8388608&localDAHStore=memory://
```

### Hash-organized Store

```text
file:///data/store?hashPrefix=2&checksum=true&logger=true
```
