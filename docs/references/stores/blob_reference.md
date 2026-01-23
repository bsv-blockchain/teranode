# Blob Store and Service Reference Documentation

## Overview

The Blob Store provides an interface for storing and retrieving binary large objects (blobs). It implements a key-value store with additional features like TTL management and range requests.

The Blob Store Service provides a HTTP interface for the Blob Store.

## Core Components

### HTTPBlobServer

The `HTTPBlobServer` struct is the main component of the Blob Store Service.

```go
type HTTPBlobServer struct {
    // store is the underlying blob storage implementation
    store Store
    // logger provides structured logging for server operations
    logger ulogger.Logger
}
```

#### Constructor

```go
func NewHTTPBlobServer(logger ulogger.Logger, storeURL *url.URL, opts ...options.StoreOption) (*HTTPBlobServer, error)
```

Creates a new `HTTPBlobServer` instance with the provided logger and store URL.

#### Methods

- `Start(ctx context.Context, addr string) error`: Starts the HTTP server on the specified address.
- `ServeHTTP(w http.ResponseWriter, r *http.Request)`: Handles incoming HTTP requests.
- `setCurrentBlockHeight(height uint32) error`: Updates the current block height in the underlying store if it supports this operation. Used for DAH (Delete-At-Height) functionality.

### Store Interface

The `Store` interface defines the contract for blob storage operations.

```go
type Store interface {
    // Health checks the health status of the blob store.
    // Parameters:
    //   - ctx: The context for the operation
    //   - checkLiveness: Whether to perform a liveness check
    // Returns:
    //   - int: HTTP status code indicating health status
    //   - string: Description of the health status
    //   - error: Any error that occurred during the health check
    Health(ctx context.Context, checkLiveness bool) (int, string, error)

    // Exists checks if a blob exists in the store.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - opts: Optional file options
    // Returns:
    //   - bool: True if the blob exists, false otherwise
    //   - error: Any error that occurred during the check
    Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)

    // Get retrieves a blob from the store.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - opts: Optional file options
    // Returns:
    //   - []byte: The blob data
    //   - error: Any error that occurred during retrieval
    Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)

    // GetIoReader returns an io.ReadCloser for streaming blob data.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - opts: Optional file options
    // Returns:
    //   - io.ReadCloser: Reader for streaming the blob data
    //   - error: Any error that occurred during setup
    GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)

    // Set stores a blob in the store.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - value: The blob data to store
    //   - opts: Optional file options
    // Returns:
    //   - error: Any error that occurred during storage
    Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error

    // SetFromReader stores a blob from an io.ReadCloser.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - reader: Reader providing the blob data
    //   - opts: Optional file options
    // Returns:
    //   - error: Any error that occurred during storage
    SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error

    // SetDAH sets the delete at height for a blob.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - dah: The delete at height
    //   - opts: Optional file options
    // Returns:
    //   - error: Any error that occurred during DAH setting
    SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error

    // GetDAH retrieves the remaining time-to-live for a blob.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob
    //   - fileType: The type of the file
    //   - opts: Optional file options
    // Returns:
    //   - uint32: The delete at height value
    //   - error: Any error that occurred during retrieval
    GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)

    // Del deletes a blob from the store.
    // Parameters:
    //   - ctx: The context for the operation
    //   - key: The key identifying the blob to delete
    //   - fileType: The type of the file
    //   - opts: Optional file options
    // Returns:
    //   - error: Any error that occurred during deletion
    Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error

    // Close closes the blob store and releases any resources.
    // Parameters:
    //   - ctx: The context for the operation
    // Returns:
    //   - error: Any error that occurred during closure
    Close(ctx context.Context) error

    // SetCurrentBlockHeight sets the current block height for the store.
    // Parameters:
    //   - height: The current block height
    SetCurrentBlockHeight(height uint32)
}
```

## HTTP Endpoints

The service exposes the following HTTP endpoints:

- `GET /health`: Check the health status of the service.
- `HEAD /blob/{key}.{fileType}`: Check if a blob exists.
- `GET /blob/{key}.{fileType}`: Retrieve a blob (supports Range headers for partial content).
- `POST /blob/{key}.{fileType}`: Store a new blob.
- `PATCH /blob/{key}.{fileType}`: Set the delete-at-height (DAH) value for a blob via `dah` query parameter.
- `DELETE /blob/{key}.{fileType}`: Delete a blob.

Note: `{key}` is a base64-encoded blob identifier and `{fileType}` is the file extension corresponding to the blob type.

## Key Features

1. **Health Checks**: The service provides a health check endpoint.
2. **Range Requests**: Supports partial content requests using the `Range` header.
3. **DAH Management**: Allows setting and retrieving Delete-At-Height values for blob lifecycle management.
4. **Streaming**: Supports streaming for both storing and retrieving blobs.
5. **Metadata Support**: Allows retrieving header and footer metadata from blobs.
6. **Atomic Writes with Error Recovery**: `SetFromReader` uses temporary files and atomic rename, with automatic cleanup on errors.

## Error Handling

The service uses HTTP status codes to indicate the result of operations:

- 200 OK: Successful operation
- 201 Created: Blob successfully stored
- 204 No Content: Blob successfully deleted
- 206 Partial Content: Range request successfully processed
- 400 Bad Request: Invalid input
- 404 Not Found: Blob not found
- 405 Method Not Allowed: Unsupported HTTP method
- 409 Conflict: Blob already exists
- 500 Internal Server Error: Server-side error

## Key Functions

- `handleHealth`: Handles health check requests.
- `handleExists`: Checks if a blob exists.
- `handleGet`: Retrieves a blob, including support for range requests.
- `handleRangeRequest`: Processes partial content requests using the Range header.
- `handleSet`: Stores a new blob.
- `handleSetDAH`: Sets the delete-at-height value for a blob.
- `handleDelete`: Deletes a blob.

## Utility Functions

- `parseRange`: Parses the `Range` header for partial content requests.
- `getKeyFromPath`: Extracts and decodes the base64-encoded blob key and file type from the request path.

## Configuration

The service can be configured with various options through the `options.StoreOption` parameter in the constructor. The server includes built-in timeouts:

- ReadTimeout: 15 seconds
- WriteTimeout: 15 seconds
- IdleTimeout: 60 seconds
- Shutdown timeout: 5 seconds

Query parameters are automatically converted to `options.FileOption` using `options.QueryToFileOptions()` for per-request configuration.

## Streaming Write Safety (SetFromReader)

The `SetFromReader` method provides safe streaming writes with automatic error recovery:

### Atomic Write Pattern

The file-based blob store implements atomic writes using temporary files:

1. Data is written to a temporary file (`.tmp` extension)
2. On success, the temp file is atomically renamed to the final filename
3. On error (including reader errors), the temp file is automatically deleted

### Abort Support via io.Pipe

When using `io.Pipe` for streaming writes, callers can abort the write by calling `pipeWriter.CloseWithError(err)`:

```go
reader, writer := io.Pipe()

go func() {
    // Write data...
    if err := someOperation(); err != nil {
        // Abort: temp file will be cleaned up
        writer.CloseWithError(err)
        return
    }
    writer.Close()  // Success: file will be finalized
}()

err := store.SetFromReader(ctx, key, fileType, reader)
```

When the pipe is closed with an error:
- `io.Copy` inside `SetFromReader` receives the error
- The defer cleanup removes the temporary file
- No incomplete data is left in storage

### Stale Temporary File Cleanup

During store initialization, the file store automatically cleans up stale `.tmp` files that are older than 10 minutes. This handles cases where the process crashed during a write operation.
