# Blob Store and Service Reference Documentation

## Overview

The Blob Store provides an interface for storing and retrieving binary large objects (blobs). It implements a key-value store with additional features like TTL management and range requests.

The Blob Store Service provides a HTTP interface for the Blob Store.

## Core Components

### HTTPBlobServer

The `HTTPBlobServer` struct is the main component of the Blob Store Service.

```go
type HTTPBlobServer struct {
store  Store
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

### Store Interface

The `Store` interface defines the contract for blob storage operations.

```go
type Store interface {
Health(ctx context.Context, checkLiveness bool) (int, string, error)
Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error)
Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error)
GetHead(ctx context.Context, key []byte, nrOfBytes int, opts ...options.FileOption) ([]byte, error)
GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error)
Set(ctx context.Context, key []byte, value []byte, opts ...options.FileOption) error
SetFromReader(ctx context.Context, key []byte, value io.ReadCloser, opts ...options.FileOption) error
SetTTL(ctx context.Context, key []byte, ttl time.Duration, opts ...options.FileOption) error
GetTTL(ctx context.Context, key []byte, opts ...options.FileOption) (time.Duration, error)
Del(ctx context.Context, key []byte, opts ...options.FileOption) error
Close(ctx context.Context) error
}
```

## HTTP Endpoints

The service exposes the following HTTP endpoints:

- `GET /health`: Check the health status of the service.
- `HEAD /blob/{key}`: Check if a blob exists.
- `GET /blob/{key}`: Retrieve a blob.
- `POST /blob/{key}`: Store a new blob.
- `PATCH /blob/{key}`: Update the TTL of a blob.
- `DELETE /blob/{key}`: Delete a blob.

## Key Features

1. **Health Checks**: The service provides a health check endpoint.
2. **Range Requests**: Supports partial content requests using the `Range` header.
3. **TTL Management**: Allows setting and updating Time-To-Live for blobs.
4. **Streaming**: Supports streaming for both storing and retrieving blobs.

## Error Handling

The service uses HTTP status codes to indicate the result of operations:

- 200 OK: Successful operation
- 201 Created: Blob successfully stored
- 204 No Content: Blob successfully deleted
- 400 Bad Request: Invalid input
- 404 Not Found: Blob not found
- 409 Conflict: Blob already exists (on creation attempts)
- 416 Range Not Satisfiable: Invalid range request
- 500 Internal Server Error: Server-side error

## Key Functions

- `handleHealth`: Handles health check requests.
- `handleExists`: Checks if a blob exists.
- `handleGet`: Retrieves a blob, including support for range requests.
- `handleSet`: Stores a new blob.
- `handleSetTTL`: Updates the TTL of an existing blob.
- `handleDelete`: Deletes a blob.

## Utility Functions

- `parseRange`: Parses the `Range` header for partial content requests.
- `getKeyFromPath`: Extracts and decodes the blob key from the request path.

## Configuration

The service can be configured with various options through the `options.StoreOption` parameter in the constructor.
