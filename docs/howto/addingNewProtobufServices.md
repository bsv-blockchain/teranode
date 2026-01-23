### How to Add a New Protobuf Service

In addition to [generating existing Protobuf files](./generatingProtobuf.md), you may need to extend the project by adding a new Protobuf service or message (e.g., defining a new RPC endpoint) or adding a dependency on a new `.proto` file. This section explains how to:

1. **Create or modify a `.proto` file** to define a new service or message.
2. **Update the `Makefile`** to ensure your new Protobuf definitions are compiled correctly.
3. **Verify the changes** by generating the necessary Go files.

#### Step 1: Define the New Protobuf Service or Endpoint

To start, you’ll need to define your new service or message by creating or modifying a `.proto` file. For example, if you want to add a new RPC method to an existing service or create a completely new service, you would write it in a `.proto` file.

For example, to add a new RPC method in `subtreevalidation_api.proto`:

```proto
service SubtreeValidationAPI {
  rpc CheckSubtreeFromBlock (CheckSubtreeFromBlockRequest) returns (CheckSubtreeFromBlockResponse) {};
  rpc NewValidationEndpoint (NewValidationRequest) returns (NewValidationResponse) {}; // New RPC method
}

message NewValidationRequest {
  string data = 1;
}

message NewValidationResponse {
  bool success = 1;
  string message = 2;
}
```

**Note:** All service names in Teranode use the `API` suffix (e.g., `SubtreeValidationAPI`, `ValidatorAPI`, `BlockchainAPI`).

You can also define a completely new service and corresponding messages in a new `.proto` file, such as `services/newservice/newservice_api/newservice_api.proto`.

#### Step 2: Update the `Makefile`

Once your new or modified `.proto` file is ready, you need to update the `Makefile` to ensure the file is included in the Protobuf generation process.

To add a new service or dependency, follow these steps:

1. **Locate the `Makefile`** in the project root.
2. **Add a new `protoc` command** for your new `.proto` file under the `gen` target.

For example, if you created a new `newservice_api.proto` file in the `services/newservice/newservice_api/` directory, add a new `protoc` command like this:

```makefile
protoc \
--proto_path=. \
--go_out=. \
--go_opt=paths=source_relative \
--go-grpc_out=. \
--go-grpc_opt=paths=source_relative \
services/newservice/newservice_api/newservice_api.proto
```

**Note:** The directory structure follows the pattern `services/<service_name>/<service_name>_api/<service_name>_api.proto`.

This ensures that the new `.proto` file is processed and generates the corresponding Go code (both the message definitions and the gRPC stubs).

#### Step 3: Regenerate the Protobuf Files

After updating the `Makefile`, you need to regenerate the Go files to reflect the changes in your new or modified `.proto` files.

Simply run the following command:

```bash
make gen
```

This will generate the necessary Go files for all services, including the newly added ones. The generated files will be located alongside the `.proto` files, following the standard naming conventions (e.g., `newservice_api.pb.go` and `newservice_api_grpc.pb.go`).

#### Step 4: Verify the Changes

Once the Go files have been generated, you can verify that the new service or endpoint is available by checking the generated `.pb.go` and `_grpc.pb.go` files. For instance, after adding a new method, you should see:

- The new RPC method in the `SubtreeValidationAPIServer` interface in the `_grpc.pb.go` file.
- The new request and response message types in the `.pb.go` file.

Here's what you might expect in `subtreevalidation_api_grpc.pb.go`:

```go
// SubtreeValidationAPIServer is the server API for SubtreeValidationAPI.
type SubtreeValidationAPIServer interface {
  CheckSubtreeFromBlock(context.Context, *CheckSubtreeFromBlockRequest) (*CheckSubtreeFromBlockResponse, error)
  NewValidationEndpoint(context.Context, *NewValidationRequest) (*NewValidationResponse, error) // New RPC method
}
```

#### Example: Creating a Complete New Service

When creating a new service from scratch, your `.proto` file should include all required elements. Here's a complete example for `services/newservice/newservice_api/newservice_api.proto`:

```proto
syntax = "proto3";

option go_package = "./;newservice_api";

package newservice_api;

import "google/protobuf/timestamp.proto";

// NewServiceAPI provides methods for the new service functionality
service NewServiceAPI {
  // HealthGRPC checks the service's health status
  rpc HealthGRPC (EmptyMessage) returns (HealthResponse) {}

  // ProcessData performs the main service operation
  rpc ProcessData (ProcessDataRequest) returns (ProcessDataResponse) {}
}

// EmptyMessage represents an empty message structure used for health check requests
message EmptyMessage {}

// HealthResponse encapsulates the service health status information
message HealthResponse {
  bool ok = 1;
  string details = 2;
  google.protobuf.Timestamp timestamp = 3;
}

// ProcessDataRequest defines the input for data processing
message ProcessDataRequest {
  string data = 1;
}

// ProcessDataResponse contains the processing result
message ProcessDataResponse {
  bool success = 1;
  string result = 2;
}
```

**Key elements to include:**

- `syntax = "proto3";` - Specifies the protobuf version
- `option go_package = "./;newservice_api";` - Required for Go code generation
- `package newservice_api;` - Package name matching the directory structure
- Import statements for dependencies (e.g., `google/protobuf/timestamp.proto`)
- Service definition with `API` suffix
- Common patterns like `HealthGRPC` endpoint (present in all services)
- Comprehensive comments for all messages and RPC methods

#### Example: Using Shared Protobuf Models

Teranode already provides shared data models in `model/model.proto` that can be used across services. To use these in your new service:

1. **Import the model** in your service's `.proto` file:

    ```proto
    import "model/model.proto";
    ```

2. **Reference the model types** in your messages:

    ```proto
    message MyServiceRequest {
      model.MiningCandidate candidate = 1;
      string additional_data = 2;
    }
    ```

If you need to add new shared models, update `model/model.proto` directly. The Makefile already includes this file in the `gen` target, so running `make gen` will regenerate the code.

**Existing shared proto files:**

- `model/model.proto` - Core data models (MiningCandidate, BlockInfo, etc.)
- `errors/error.proto` - Error handling structures
- `stores/utxo/status.proto` - UTXO status definitions

#### Step 5: Implement the Service Interface

After generating the protobuf files, you need to implement the service interface in your Go code. Create an implementation file (e.g., `services/newservice/service.go`):

```go
package newservice

import (
    "context"
    "github.com/bsv-blockchain/teranode/services/newservice/newservice_api"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements the NewServiceAPIServer interface
type Service struct {
    newservice_api.UnimplementedNewServiceAPIServer
    // Add your service dependencies here
}

// NewService creates a new instance of the service
func NewService() *Service {
    return &Service{}
}

// HealthGRPC implements the health check endpoint
func (s *Service) HealthGRPC(ctx context.Context, req *newservice_api.EmptyMessage) (*newservice_api.HealthResponse, error) {
    return &newservice_api.HealthResponse{
        Ok:        true,
        Details:   "Service is running",
        Timestamp: timestamppb.Now(),
    }, nil
}

// ProcessData implements the main service operation
func (s *Service) ProcessData(ctx context.Context, req *newservice_api.ProcessDataRequest) (*newservice_api.ProcessDataResponse, error) {
    // Implement your business logic here
    return &newservice_api.ProcessDataResponse{
        Success: true,
        Result:  "Processed: " + req.Data,
    }, nil
}
```

**Next steps:**

1. Register the service with the gRPC server in your main application
2. Add configuration for the service in `settings.conf`
3. Write unit tests for your service implementation
4. Add integration tests if needed

#### Step 6: Cleaning Up Generated Files (Optional)

If you need to remove the previously generated files for some reason (e.g., during refactoring), you can use the following command:

```bash
make clean_gen
```

This will delete all generated `.pb.go` files, allowing you to start fresh when running `make gen` again.

#### Additional Makefile Commands

For more information about the `Makefile` and additional commands you can use, please refer to the [Makefile Documentation](./makefile.md).

---
