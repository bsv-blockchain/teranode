# 📘 Error Handling


## Index


1. [Introduction](#1-introduction)
- [1.1. Go Errors](#11-go-errors)
- [1.2. Go Errors Best Practices](#12-go-errors---best-practices)
- [1.3. Sentinel Errors](#13-sentinel-errors)
- [1.4 Wrapping Errors](#14-wrapping-errors)
2. [Error Handling in Teranode](#2-error-handling-in-teranode)
- [2.1. Error Handling Strategy](#21-error-handling-strategy)
- [2.2. Sentinel Errors in Teranode](#22-sentinel-errors-in-teranode)
- [2.3. Error Wrapping in Teranode](#23-error-wrapping-in-teranode)
- [2.4. gRPC Error Wrapping in Teranode](#24-grpc-error-wrapping-in-teranode)
- [2.6. Extra Data](#26-extra-data)
- [2.7. Error Protobuf](#27-error-protobuf)
- [2.8. Unit Tests](#28-unit-tests)


## 1. Introduction

### 1.1. Go Errors
In Go (Golang), error handling is managed through an interface called `error`. This interface is defined in the built-in `errors` package.

The `error` interface in Go is defined as follows:

```go
type error interface {
    Error() string
}
```

Any type that implements this `Error() string` method satisfies the `error` interface. This "convention over configuration" approach encourages Go programs to handle errors explicitly by checking whether an error is nil before proceeding with normal operations.

Custom error types can be created by defining types that implement the `error` interface. This is useful for conveying error context or state beyond just a text message. Here’s a simple example:

```go
type MyError struct {
    Msg string
    File string
    Line int
}

func (e *MyError) Error() string {
    return fmt.Sprintf("%s:%d: %s", e.File, e.Line, e.Msg)
}
```

Functions that can result in an error typically return an error object as their last return value. The calling function should always check this error before proceeding.

```go
result, err := someFunction()
if err != nil {
    // handle error
    log.Fatal(err)
}
// proceed with using result
```

This pattern encourages handling errors at the place they occur, rather than propagating them up the stack implicitly via exceptions.


### 1.2. Go Errors - Best Practices

- Always check for errors where they might occur. Do not ignore returned error values or propagate them up.
- Consider defining your custom error types for complex systems, which can add clarity and control in error handling.
- Use standard `errors.Is` (to check for a specific errors) and `errors.As` (to check for type compatibility and type assertion).
- Parsing the text of an error message to determine the type or cause of an error is considered an anti-pattern. Instead, use custom error types or error wrapping to provide context.

### 1.3. Sentinel Errors

Sentinel errors in Go are predefined error variables that represent specific error conditions. These are declared at the package level and used consistently throughout the code to signify particular states or outcomes. This pattern is useful for comparing returned errors to known values, providing a consistent method for error checking.

Since sentinel errors are variables, they can easily be compared using `errors.Is()` to check if a particular error has occurred.
Using sentinel errors keeps error handling simple and explicit, requiring only basic checks against predefined values.


Here's a basic example of defining and using sentinel errors in a Go program:

```go
package main

import (
    "errors"
    "fmt"
)

// Define sentinel errors
var (
    ErrNotFound = errors.New("not found")
    ErrPermissionDenied = errors.New("permission denied")
)

func findItem(id string) error {
    // Simulated condition: item not found
    if id != "expected_id" {
        return ErrNotFound
    }
    return nil
}

func main() {
    err := findItem("unknown_id")
    if errors.Is(err, ErrNotFound) {
        fmt.Println("Item not found!")
    } else if errors.Is(err, ErrPermissionDenied) {
        fmt.Println("Access denied.")
    } else if err != nil {
        fmt.Println("An unexpected error occurred:", err)
    } else {
        fmt.Println("Item found.")
    }
}
```


While useful, sentinel errors have limitations, especially as applications scale. Code that uses sentinel errors is tightly coupled to these errors, making changes to error definitions potentially disruptive.

### 1.4 Wrapping Errors

In Go, error wrapping is a technique used to add additional context to an error while preserving the original error itself. This approach helps maintain a "chain" of errors, which can be useful for debugging and error handling in complex microservices.

Example of wrapping an error:

```go
package main

import (
    "errors"
    "fmt"
)

func operation1() error {
    return errors.New("base error")
}

func operation2() error {
    err := operation1()
    if err != nil {
        // Wrap the error with additional context
        return fmt.Errorf("operation2 failed: %w", err)
    }
    return nil
}

func main() {
    err := operation2()
    if err != nil {
        fmt.Println("An error occurred:", err)
    }
}

```

On the other hand, unwrapping is the process of retrieving the original error from a wrapped error. You can unwrap errors manually using the `Unwrap` method provided by the `errors` package, or you can use higher-level utilities like `errors.Is` and `errors.As` to check for specific errors or extract errors of specific types.

In this context, `errors.Is` and `errors.As` are used to check errors in an error chain without needing to manually call `Unwrap` repeatedly

- `errors.Is`: checks whether any error in the chain matches a specific error.
- `errors.As`: finds the first error in the chain that matches a specific type and provides access to it.

Benefits of Wrapping Errors:

1. Wrapping errors helps in preserving the context where an error occurred, without losing information about the original error.
2.  Maintaining an error chain aids in diagnosing issues by providing a traceable path of what went wrong and where.
3. Allows developers to decide how much error information to expose to different parts of the application or to the end user, enhancing security and usability.


## 2. Error Handling in Teranode


### 2.1. Error Handling Strategy

Teranode follows a structured error handling strategy that combines the use of sentinel errors and error wrapping to ensure clear, consistent, and traceable error handling throughout the application.

The `errors/Error.go` file contains the definition of sentinel errors and wrapping / unwrapping functions used across the application.

An error is defined as a struct containing a code error, a message, an optional wrapped error and an optional extra data (`Data`) .

```go
type Error struct {
    Code       ERR
    Message    string
    WrappedErr error
    Data       ErrData
}
```

### 2.2. Sentinel Errors in Teranode

As mentioned in previous sections, sentinel errors are predefined errors that serve as fixed references for common error conditions. They are an integral part of the Teranode error handling strategy, providing a standard and efficient way to recognize and manage common specific error scenarios.

**Code Example:**

Here we can see how the sentinel errors are defined in the `errors/Error.go` file:

```go
package errors

var (
    ErrNotFound             = New(ERR_NOT_FOUND, "not found")
    ErrInvalidArgument      = New(ERR_INVALID_ARGUMENT, "invalid argument")
    ErrThresholdExceeded    = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
    // Other sentinel errors follow...
)
```

Each sentinel error is created using a `New` function which ensures that each error has a unique code and a message that describes the error succinctly.


These sentinel errors are particularly useful for error comparisons and decision-making in the Teranode business logic. By comparing the returned errors to these predefined values, functions can determine the next steps without ambiguity.

**How to Use Example:**

When a function encounters an error, it can return one of these predefined errors. Here’s an example of how a function might use sentinel errors:

```go
func fetchData(id string) (*Data, error) {
    if id == "" {
        return nil, ErrInvalidArgument
    }
    data, found := database.Find(id)
    if !found {
        return nil, ErrNotFound
    }
    return data, nil
}

// In another part of the application, you can check the error:
data, err := fetchData("")
if err != nil {
    if errors.Is(err, ErrInvalidArgument) {
        fmt.Println("Invalid ID provided.")
    } else if errors.Is(err, ErrNotFound) {
        fmt.Println("Data not found.")
    } else {
        fmt.Println("An unexpected error occurred:", err)
    }
}
```

In this example, `fetchData` checks if the provided ID is empty and uses `ErrInvalidArgument` to indicate this problem. The calling code then checks the type of error using `errors.Is`, which simplifies handling specific errors accordingly.


### 2.3. Error Wrapping in Teranode

Error wrapping in Teranode allows to create nested errors and propagate them through the different layers of the application. This allows an error to carry its history along with new context, effectively creating a chain of errors that leads back to the original issue.

By maintaining a trail of errors, developers can trace back through the execution flow to understand what led to the error.
Additionally, different layers of the application can decide how to handle errors based on their type and origin.

The `New` function in Teranode's error package creates and returns a pointer to an `Error` struct, which includes fields for the error code, a message, and an optional wrapped error.


```go
func New(code ERR, message string, params ...interface{}) *Error {
	var wErr error

	// sprintf the message with the params except the last one if the last one is an error
	if len(params) > 0 {
		if err, ok := params[len(params)-1].(error); ok {
			wErr = err
			params = params[:len(params)-1]
		}
	}

	if len(params) > 0 {
		message = fmt.Sprintf(message, params...)
	}

	// Check the code exists in the ErrorConstants enum
	if _, ok := ERR_name[int32(code)]; !ok {
		return &Error{
			Code:       code,
			Message:    "invalid error code",
			WrappedErr: wErr,
		}
	}

	return &Error{
		Code:       code,
		Message:    message,
		WrappedErr: wErr,
	}
}
```

1. The function `New` takes an error code, a message, and a variadic `params` slice which may include one error that needs to be wrapped.

2. If the last parameter in `params` is an error, it is treated as the error to be wrapped. This error is then stored in the `WrappedErr` field of the `Error` struct, and any other parameters are used to format the error message.

3. If there are additional parameters (other than the error to be wrapped), they are used to format the message string using `fmt.Sprintf`, allowing dynamic message content based on runtime values.

Here is a practical example of using the `New` function to create and wrap errors:

```go
func someOperation() error {
    err := doSomethingThatMightFail()
    if err != nil {
        // Wrap the error with additional context and return
        return New(ErrInvalidArgument, "operation failed due to underlying error: %v", err)
    }
    return nil
}
```

In this scenario, `someOperation` calls another function and wraps any error returned by this function with additional context, using the custom `New` function.


The `Unwrap` method in the `Error` structure returns the error that was wrapped within the current error, if any.

```go
func (e *Error) Unwrap() error {
    return e.WrappedErr
}
```

This method allows the use of Go's built-in `errors.Unwrap` function to continue unwrapping through multiple layers of nested wrapped errors, should there be more than one.

To effectively utilize the unwrapping functionality, you can use a loop or a conditional check to explore the chain of errors. Here’s an example that demonstrates how to use the `Unwrap` method to trace back through wrapped errors:

**Practical Example:**

```go
// Assuming `someOperation` returns an error that may be wrapped multiple times
err := someOperation()
for err != nil {
    fmt.Printf("Error encountered: %v\n", err)
    // Attempt to unwrap the error
    if unwrappedErr := errors.Unwrap(err); unwrappedErr != nil {
        err = unwrappedErr
    } else {
        break
    }
}
```

In this example, `errors.Unwrap` is used in a loop to keep unwrapping the error until no more wrapped errors are found, effectively reaching the original error. This approach is particularly useful when you need to diagnose an issue and understand the sequence of errors that led to the final state.

Best Practices:

1. Unwrap errors only when necessary. For example, if you're handling specific error types differently, unwrap errors until you find the type you're looking for.
2. Always log or monitor the full error chain before unwrapping in production environments to preserve the context and details of what went wrong.



### 2.4. gRPC Error Wrapping in Teranode


When working with distributed systems that utilize gRPC, errors generated by one service must be communicated effectively to other services. Wrapping and unwrapping errors for gRPC enables Teranode to maintain consistent error handling practices even when errors cross service boundaries.

This involves converting the custom error types of Teranode into gRPC-compatible errors, which use the `status` package from `google.golang.org/grpc`.

The wrapping of gRPC errors can be seen in the `WrapGRPC` function within the `Error.go`.

```go
package errors

import (
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/types/known/anypb"
)

...
...

func WrapGRPC(err error) error {
    if err == nil {
        return nil
    }

    var uErr *Error
    if errors.As(err, &uErr) {
        details, _ := anypb.New(&TError{
            Code:    uErr.Code,
            Message: uErr.Message,
        })
        st := status.New(ErrorCodeToGRPCCode(uErr.Code), uErr.Message)
        st, err := st.WithDetails(details)
        if err != nil {
            return status.New(codes.Internal, "error adding details to gRPC status").Err()
        }
        return st.Err()
    }
    return status.New(ErrorCodeToGRPCCode(ErrUnknown.Code), ErrUnknown.Message).Err()
}
```

`WrapGRPC` converts an internal Teranode error into a gRPC `status`, embedding additional details as needed. This allows the receiving service to understand not only the nature of the error but also to receive contextual metadata.


When a Teranode service receives a gRPC error, it needs to convert it back into an internal error format. This process, known as unwrapping, involves extracting information from the gRPC error and reconstructing the original Teranode error as closely as possible.

The unwrapping of gRPC errors can be seen in the `UnwrapGRPC` function within the `Error.go`.

```go
package errors

import (
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/known/anypb"
)

...
...

func UnwrapGRPC(err error) error {
    if err == nil {
        return nil
    }

    st, ok := status.FromError(err)
    if !ok {
        return err // Not a gRPC status error
    }

    for _, detail := range st.Details() {
        var ubsvErr TError
        if err := anypb.UnmarshalTo(detail.(*anypb.Any), &ubsvErr, proto.UnmarshalOptions{}); err == nil {
            return New(ubsvErr.Code, ubsvErr.Message)
        }
    }

    return New(ErrUnknown.Code, st.Message())
}
```

`UnwrapGRPC` checks if the error is a gRPC status error, extracts details embedded in the status, and attempts to reconstruct a Teranode-specific error using these details.

Note: for this to be effective, a clear mapping between Teranode error codes and gRPC status codes must be maintained, in order to ensure that error meanings are preserved across service boundaries.

Just to see an end to end an example of wrapping and unwrapping gRPC errors, when the Blockchain Server.go receives a gRPC request to provide a block (`GetBlock`), it may encounter an error while processing the request. This error is then wrapped into a gRPC status error before being returned to the client.

```go
	blockHash, err := chainhash.NewHash(request.Hash)
    if err != nil {
        return nil, errors.WrapGRPC(errors.New(errors.ERR_BLOCK_NOT_FOUND, "[Blockchain] request's hash is not valid", err))
    }
```

This is then interpreted by the Client (`GetBlock` in the Blockchain `Client.go`) as follows:

```go
	resp, err := c.client.GetBlock(ctx, &blockchain_api.GetBlockRequest{
		Hash: blockHash[:],
	})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}
```

From this point on, the service invoking the blockchain client can handle the error as a Teranode error, even though it was originally delivered as a gRPC error.


### 2.6. Extra Data

The `Data` field in the `Error` struct is an interface type `ErrData`. This interface is defined as follows:

```go
type ErrData interface {
    Error() string
}
```

This means that any type that implements the `Error()` method (returns a string) can be used as the `Data` field in the `Error` struct. The purpose of having a `Data` field in the error struct is to allow attaching additional contextual information or payload to the error.
Notice how this is different from a wrapped error.

We can see an example here:


```go

type UtxoSpentErrData struct {
    Hash           chainhash.Hash
    SpendingTxHash chainhash.Hash
    Time           time.Time
}

func (e *UtxoSpentErrData) Error() string {
    return fmt.Sprintf("utxo %s already spent by %s at %s", e.Hash, e.SpendingTxHash, e.Time)
}

func NewUtxoSpentErr(txID chainhash.Hash, spendingTxID chainhash.Hash, t time.Time, err error) *Error {

	// 1 - Create a new error
	e := New(ERR_TX_ALREADY_EXISTS, "utxoSpentErrStruct.Error()", err)

	// 2 - Create a second error, to be used for the Data field
    utxoSpentErrStruct := &UtxoSpentErrData{
        Hash:           txID,
        SpendingTxHash: spendingTxID,
        Time:           t,
    }

	// 3 - Attach the data to the original error
	e.Data = utxoSpentErrStruct

	return e
}
```

In this example, we can see:
* In the `NewUtxoSpentErr` function, a new custom error (`*Error`) is created using the `New` function, together with a `ERR_TX_ALREADY_EXISTS` sentinel code.
* To provide a more descriptive error, a new `UtxoSpentErrData` struct is created.
* The `Data` field of the error (`e.Data`) is set to the `utxoSpentErrStruct`, attaching the UtxoSpentErrData to the error.



### 2.7. Error Protobuf

`error.proto` defines a protocol buffer message for error handling in Teranode. This file specifies the structure of error messages, including error codes and messages, using the Protobuf language.

The `enum ERR` defines an enumeration of possible error codes with explicit values (e.g., `UNKNOWN = 0; INVALID_ARGUMENT = 1;`). These enums help standardize error handling across different Teranode services that interact with each other.

Use `protoc-gen-go` to compile the proto file.

### 2.8. Unit Tests

Extensive unit tests are available under the `errors` package (`Error_test.go`). Should you add any new functionality or scenario, it is recommended to update the unit tests to ensure that the error handling logic remains consistent and reliable.
