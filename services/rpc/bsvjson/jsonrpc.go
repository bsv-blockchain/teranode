// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package bsvjson implements the Bitcoin JSON-RPC protocol used for client-server communication.
//
// This package provides the core types and functions for working with Bitcoin's JSON-RPC protocol,
// which follows a request-response pattern where clients send commands to the server and
// receive structured responses or errors. The package implements:
//
// - Standard JSON-RPC envelope types (Request, Response)
// - Error handling with standardized error codes and messages
// - Type-safe marshaling and unmarshaling between Go and JSON
// - Bitcoin-specific RPC command structures and serialization
//
// The bsvjson package is designed to be used by both clients (constructing requests)
// and servers (parsing requests and generating responses), with a focus on type safety
// and validation to prevent protocol-level errors.
//
// Security considerations:
// - Proper validation of request IDs to prevent injection attacks
// - Structured error system to avoid leaking sensitive information
// - Type checking to ensure protocol compliance
//
// This package forms the foundation of the RPC service's communication layer and is
// critical for ensuring reliable and secure interactions with external clients.
package bsvjson

import (
	"encoding/json"
	"fmt"
)

// RPCErrorCode represents a standardized error code used in Bitcoin JSON-RPC responses.
// These codes follow the JSON-RPC 2.0 specification along with Bitcoin-specific extensions.
//
// Using a specific integer type rather than raw integers helps ensure type safety and
// prevents using incorrect or non-standard error codes in responses. This makes
// error handling more consistent and reliable for clients parsing the responses.
//
// Error codes are organized into ranges:
// - Standard JSON-RPC errors: -32700 to -32603
// - Bitcoin protocol errors: -1 to -25 (and more)
// - Teranode-specific errors: Custom range for node-specific conditions
type RPCErrorCode int

// RPCError represents a structured error object that conforms to the JSON-RPC specification.
// It encapsulates both an error code and a human-readable message that describes the
// nature of the error for client interpretation and display.
//
// RPCError implements the standard Go error interface, allowing it to be used
// seamlessly with standard error handling patterns while providing additional
// structured data for protocol-aware clients.
//
// This type appears in JSON-RPC Response objects when an error occurs during
// command processing, providing clients with machine-readable error information.
type RPCError struct {
	// Code is a standardized error code that identifies the type of error
	Code    RPCErrorCode `json:"code,omitempty"`

	// Message is a human-readable description of the error
	Message string       `json:"message,omitempty"`
}

// Guarantee RPCError satisifies the builtin error interface.
var _, _ error = RPCError{}, (*RPCError)(nil)

// Error returns a string describing the RPC error.  This satisifies the
// builtin error interface.
func (e RPCError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

// NewRPCError constructs and returns a new JSON-RPC error that is suitable
// for use in a JSON-RPC Response object.
func NewRPCError(code RPCErrorCode, message string) *RPCError {
	return &RPCError{
		Code:    code,
		Message: message,
	}
}

// IsValidIDType checks that the ID field (which can go in any of the JSON-RPC
// requests, responses, or notifications) is valid.  JSON-RPC 1.0 allows any
// valid JSON type.  JSON-RPC 2.0 (which bitcoind follows for some parts) only
// allows string, number, or null, so this function restricts the allowed types
// to that list.  This function is only provided in case the caller is manually
// marshalling for some reason.    The functions which accept an ID in this
// package already call this function to ensure the provided id is valid.
func IsValidIDType(id interface{}) bool {
	switch id.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		string,
		nil:
		return true
	default:
		return false
	}
}

// Request represents a JSON-RPC request object that follows the Bitcoin RPC protocol specification.
// It encapsulates all the data needed to identify, validate, and execute a remote procedure call.
//
// The Bitcoin RPC protocol is based on JSON-RPC 1.0 with some Bitcoin-specific extensions
// and modifications. Each request contains:
// - A method name identifying the command to execute
// - Parameters specific to that command
// - A unique ID to correlate requests with responses
// - Optional JSON-RPC version indicator
//
// While this package provides higher-level command types with static typing for most
// RPC operations, this raw Request type is exported for:
// - Custom extensions to the protocol
// - Direct protocol-level manipulation
// - Testing and debugging purposes
// - Handling commands not covered by the static command types
//
// The Params field uses json.RawMessage to allow handling of the varied parameter
// types that different commands require, deferring unmarshaling until the specific
// command type is known.
type Request struct {
	// Jsonrpc optionally identifies the JSON-RPC protocol version
	Jsonrpc string            `json:"jsonrpc"`

	// Method identifies the remote procedure (command) to be called
	Method  string            `json:"method"`

	// Params contains the parameters for the command as an array of raw JSON messages
	Params  []json.RawMessage `json:"params"`

	// ID is a client-generated identifier to match responses with requests
	// It can be a string, number, or null according to the JSON-RPC spec
	ID      interface{}       `json:"id"`
}

// NewRequest constructs a new JSON-RPC request object with the specified identifier,
// method name, and parameters.
//
// This function creates a properly formatted JSON-RPC request that follows the
// Bitcoin RPC protocol specifications. It handles the serialization of parameters
// into the appropriate format and validates the request ID for protocol compliance.
//
// The parameters must be marshalable to JSON, and each parameter will be converted
// to a json.RawMessage in the resulting Request object. This allows for parameters
// of any valid JSON type (objects, arrays, strings, numbers, booleans, or null).
//
// Parameters:
//   - id: A unique identifier for the request (string, int, etc.) that will be echoed in the response
//   - method: The name of the RPC method to invoke on the server
//   - params: An array of parameters to pass to the method, which will be marshaled to JSON
//
// Returns:
//   - *Request: A properly constructed JSON-RPC request object if successful
//   - error: An error if parameter marshaling fails or the ID is invalid
//
// Usage Notes:
// While this function provides low-level control over request creation, most callers
// should prefer using the type-specific command constructors (e.g., NewGetBlockCmd)
// along with MarshalCmd to create requests with proper type checking.
func NewRequest(id interface{}, method string, params []interface{}) (*Request, error) {
	if !IsValidIDType(id) {
		str := fmt.Sprintf("the id of type '%T' is invalid", id)
		return nil, makeError(ErrInvalidType, str)
	}

	rawParams := make([]json.RawMessage, 0, len(params))

	for _, param := range params {
		marshalledParam, err := json.Marshal(param)
		if err != nil {
			return nil, err
		}

		rawMessage := json.RawMessage(marshalledParam)
		rawParams = append(rawParams, rawMessage)
	}

	return &Request{
		Jsonrpc: "1.0",
		ID:      id,
		Method:  method,
		Params:  rawParams,
	}, nil
}

// Response encapsulates a JSON-RPC response according to the Bitcoin RPC protocol specification.
// It contains either a successful result or an error, along with an ID that correlates
// the response to the original request.
//
// The Response structure follows JSON-RPC standards with Bitcoin-specific conventions:
// - Only one of Result or Error will be populated for a given response
// - The Result field contains command-specific data on success
// - The Error field contains structured error information on failure
// - The ID field matches the ID sent in the corresponding request
//
// This structure handles the variability of different command results by using
// json.RawMessage for the Result field, allowing type-specific unmarshaling
// to be performed by the caller based on the expected result type.
//
// Security considerations:
// - Error messages should be carefully constructed to avoid leaking sensitive information
// - IDs must be properly validated to prevent injection attacks
// - Result data may need sanitization depending on its contents
type Response struct {
	// Result contains the command result data on success (nil on error)
	// The content varies by command and must be unmarshaled to a specific type
	Result json.RawMessage `json:"result"`

	// Error contains structured error information when a command fails (nil on success)
	Error  *RPCError       `json:"error"`

	// ID matches the ID from the request to correlate responses with requests
	// It's a pointer to allow null values in the JSON representation
	ID     *interface{}    `json:"id"`
}

// NewResponse constructs a new JSON-RPC response object with the specified identifier,
// result data, and error information.
//
// This function creates a properly formatted JSON-RPC response that follows the
// Bitcoin RPC protocol specifications. It handles the correlation between requests
// and responses through the ID field and ensures that the response structure
// conforms to the expected format for Bitcoin clients.
//
// According to the JSON-RPC specification, responses must include either a result
// or an error, but not both. This function will prioritize the error if both
// are provided.
//
// Parameters:
//   - id: The identifier matching the original request (must be valid per IsValidIDType)
//   - marshalledResult: The pre-marshalled JSON result data (ignored if rpcErr is not nil)
//   - rpcErr: An optional RPC error object to include in the response (nil for success responses)
//
// Returns:
//   - *Response: A properly constructed JSON-RPC response object if successful
//   - error: An error if ID validation fails
//
// Usage Notes:
// While this function provides low-level control over response creation, most server
// implementations should prefer using MarshalResponse to directly generate the
// complete JSON response in one step.
func NewResponse(id interface{}, marshalledResult []byte, rpcErr *RPCError) (*Response, error) {
	if !IsValidIDType(id) {
		str := fmt.Sprintf("the id of type '%T' is invalid", id)
		return nil, makeError(ErrInvalidType, str)
	}

	pid := &id

	return &Response{
		Result: marshalledResult,
		Error:  rpcErr,
		ID:     pid,
	}, nil
}

// MarshalResponse creates a complete JSON-RPC response message ready for transmission
// to a client by marshaling the provided identifier, result, and error.
//
// This function handles the entire process of building a JSON-RPC response including:
// 1. Validating the request ID for protocol compliance
// 2. Marshaling the result data to JSON (for success responses)
// 3. Constructing a Response object with the appropriate fields
// 4. Marshaling the entire Response to a JSON byte slice
//
// The function follows Bitcoin JSON-RPC protocol semantics where:
// - Only one of result or error will be present in any response
// - The error takes precedence if both are provided
// - IDs must match between request and response for correlation
//
// Parameters:
//   - id: The identifier matching the original request
//   - result: The command result data to marshal (ignored if rpcErr is not nil)
//   - rpcErr: An optional RPC error object to include (nil for success responses)
//
// Returns:
//   - []byte: The fully marshaled JSON-RPC response ready for transmission
//   - error: Any error encountered during the marshaling process
//
// This is the preferred function for generating complete RPC responses for
// transmission to clients from server implementations.
func MarshalResponse(id interface{}, result interface{}, rpcErr *RPCError) ([]byte, error) {
	marshalledResult, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}

	response, err := NewResponse(id, marshalledResult, rpcErr)
	if err != nil {
		return nil, err
	}

	return json.Marshal(&response)
}
