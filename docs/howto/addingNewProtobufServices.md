### How to Add a New Protobuf Service

In addition to [generating existing Protobuf files](./generatingProtobuf.md), you may need to extend the project by adding a new Protobuf service or message (e.g., defining a new RPC endpoint) or adding a dependency on a new `.proto` file. This section explains how to:

1. **Create or modify a `.proto` file** to define a new service or message.
2. **Update the `Makefile`** to ensure your new Protobuf definitions are compiled correctly.
3. **Verify the changes** by generating the necessary Go files.

#### Step 1: Define the New Protobuf Service or Endpoint

To start, you’ll need to define your new service or message by creating or modifying a `.proto` file. For example, if you want to add a new RPC method to an existing service or create a completely new service, you would write it in a `.proto` file.

For example, to add a new RPC method in `subtreevalidation_api.proto`:

```proto
service SubtreeValidationService {
rpc ValidateSubtree (SubtreeValidationRequest) returns (SubtreeValidationResponse);
rpc NewValidationEndpoint (NewValidationRequest) returns (NewValidationResponse); // New RPC method
}

message NewValidationRequest {
string data = 1;
}

message NewValidationResponse {
bool success = 1;
string message = 2;
}
```

You can also define a completely new service and corresponding messages in a new `.proto` file, such as `services/newservice/newservice_api.proto`.

#### Step 2: Update the `Makefile`

Once your new or modified `.proto` file is ready, you need to update the `Makefile` to ensure the file is included in the Protobuf generation process.

To add a new service or dependency, follow these steps:

1. **Locate the `Makefile`** in the project root.
2. **Add a new `protoc` command** for your new `.proto` file under the `gen` section.

For example, if you created a new `newservice_api.proto` file in the `services/newservice/` directory, add a new `protoc` command like this:

```makefile
protoc \
--proto_path=. \
--go_out=. \
--go_opt=paths=source_relative \
--go-grpc_out=. \
--go-grpc_opt=paths=source_relative \
services/newservice/newservice_api.proto
```

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

- The new RPC method in the `SubtreeValidationServiceServer` interface in the `_grpc.pb.go` file.
- The new request and response message types in the `.pb.go` file.

Here’s what you might expect in `subtreevalidation_api_grpc.pb.go`:

```go
// SubtreeValidationServiceServer is the server API for SubtreeValidationService.
type SubtreeValidationServiceServer interface {
ValidateSubtree(context.Context, *SubtreeValidationRequest) (*SubtreeValidationResponse, error)
NewValidationEndpoint(context.Context, *NewValidationRequest) (*NewValidationResponse, error) // New RPC method
}
```

#### Example: Adding a New Protobuf Dependency

Let’s say you want to add a new `.proto` file that defines a shared data model used across several services. This file could be called `common.proto` and might look like this:

```proto
syntax = "proto3";

package common;

message CommonMessage {
string id = 1;
string content = 2;
}
```

To add this new `common.proto` file to your project and make it available to other services, follow these steps:

1. **Add the `common.proto` file** to the `model` or `shared` directory (or another appropriate location).
2. **Update the `Makefile`** by adding a `protoc` command to generate the Go files for `common.proto`.

For example:
```makefile
protoc \
--proto_path=. \
--go_out=. \
--go_opt=paths=source_relative \
model/common.proto
```

This ensures that any service depending on `common.proto` can now reference the generated Go code.

#### Step 5: Cleaning Up Generated Files (Optional)

If you need to remove the previously generated files for some reason (e.g., during refactoring), you can use the following command:

```bash
make clean_gen
```

This will delete all generated `.pb.go` files, allowing you to start fresh when running `make gen` again.

#### Additional Makefile Commands

For more information about the `Makefile` and additional commands you can use, please refer to the [Makefile Documentation](./makefile.md).

---
