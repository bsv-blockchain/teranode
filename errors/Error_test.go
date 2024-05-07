package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// Test_NewCustomError tests the creation of custom errors.
func Test_NewCustomError(t *testing.T) {
	err := New(ERR_NOT_FOUND, "resource not found")
	require.NotNil(t, err)
	require.Equal(t, ERR_NOT_FOUND, err.Code)
	require.Equal(t, "resource not found", err.Message)

	secondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", err)
	thirdErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", secondErr)
	anotherErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "Another ERR, block is invalid")
	fourthErr := New(ERR_SERVICE_ERROR, "older error: ", thirdErr)
	fifthErr := New(ERR_BLOCK_INVALID, "invalid tx double spend error", fourthErr)

	require.True(t, anotherErr.Is(thirdErr))
	require.True(t, fourthErr.Is(New(ERR_TX_INVALID_DOUBLE_SPEND, "")))
	require.True(t, fourthErr.Is(ErrTxInvalidDoubleSpend))

	require.True(t, fourthErr.Is(err))
	require.True(t, fifthErr.Is(thirdErr))
	require.True(t, fifthErr.Is(err))

	require.False(t, anotherErr.Is(fourthErr))
	require.False(t, fifthErr.Is(ErrBlockNotFound))

}

func Test_FmtErrorCustomError(t *testing.T) {
	err := New(ERR_NOT_FOUND, "resource not found")
	require.NotNil(t, err)
	require.Equal(t, ERR_NOT_FOUND, err.Code)
	require.Equal(t, "resource not found", err.Message)

	fmtError := fmt.Errorf("error: %w", err)
	require.NotNil(t, fmtError)
	secondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", fmtError)
	require.NotNil(t, secondErr)

	// If we FMT Err, then they won't be recognized as equal
	require.False(t, secondErr.Is(err))

	altErr := New(ERR_INVALID_ARGUMENT, "invalid argument", err)
	altSecondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", fmtError)
	require.True(t, altSecondErr.Is(altErr))
}

// Test_WrapGRPC tests wrapping a custom error for gRPC.
func Test_WrapGRPC(t *testing.T) {
	originalErr := New(ERR_NOT_FOUND, "not found")
	wrappedErr := WrapGRPC(originalErr)
	s, ok := status.FromError(wrappedErr)
	if !ok {
		t.Fatalf("expected gRPC status error; got %T", wrappedErr)
	}

	if s.Code() != codes.Internal {
		t.Errorf("expected gRPC code %v; got %v", codes.Internal, s.Code())
	}
}

// TestUnwrapGRPC tests unwrapping a gRPC error back to a custom error.
func Test_UnwrapGRPC(t *testing.T) {
	// Simulate gRPC error
	grpcErr := status.Error(codes.NotFound, "not found")

	// Unwrap
	unwrappedErr := UnwrapGRPC(grpcErr)

	// Check error properties
	if !errors.Is(unwrappedErr, New(ERR_NOT_FOUND, "")) {
		t.Errorf("unwrapped error does not match expected type or properties, it is: %s", unwrappedErr.Error())
	}
}

func Test_ErrorIs(t *testing.T) {
	err := New(ERR_NOT_FOUND, "not found")
	if !errors.Is(err, New(ERR_NOT_FOUND, "")) {
		t.Errorf("errors.Is failed to recognize NOT_FOUND error type")
	}

	err = New(ERR_BLOCK_INVALID, "invalid block error")
	if !errors.Is(err, New(ERR_BLOCK_INVALID, "")) {
		t.Errorf("errors.Is failed to recognize INVALID_BLOCK error type")
	}

	err = New(ERR_TX_INVALID_DOUBLE_SPEND, "invalid tx double spend error")
	if !errors.Is(err, New(ERR_TX_INVALID_DOUBLE_SPEND, "")) {
		t.Errorf("errors.Is failed to recognize INVALID_TX_DOUBLE_SPEND error type")
	}

	err = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded error")
	if !errors.Is(err, New(ERR_THRESHOLD_EXCEEDED, "")) {
		t.Errorf("errors.Is failed to recognize THRESHOLD_EXCEEDED error type")
	}

	err = New(ERR_BLOCK_NOT_FOUND, "block not found error")
	if !errors.Is(err, New(ERR_BLOCK_NOT_FOUND, "")) {
		t.Errorf("errors.Is failed to recognize BLOCK_NOT_FOUND error type")
	}

	err = New(ERR_UNKNOWN, "unknown error")
	if !errors.Is(err, New(ERR_UNKNOWN, "")) {
		t.Errorf("errors.Is failed to recognize UNKNOWN error type")
	}

	err = New(ERR_INVALID_ARGUMENT, "invalid argument error")
	if !errors.Is(err, New(ERR_INVALID_ARGUMENT, "")) {
		t.Errorf("errors.Is failed to recognize INVALID_ARGUMENT error type")
	}
}

func Test_ErrorWrapWithAdditionalContext(t *testing.T) {
	originalErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "original error")
	wrappedErr := New(ERR_BLOCK_INVALID, "Some more additional context", originalErr)

	if !errors.Is(wrappedErr, originalErr) {
		t.Errorf("Wrapped error does not match original error")
	}

	if !strings.Contains(wrappedErr.Error(), "Some more additional context") {
		t.Errorf("Wrapped error does not contain additional context")
	}
}

func Test_ErrorEquality(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "resource not found")
	err2 := New(ERR_NOT_FOUND, "resource not found")

	if !err1.Is(err2) {
		t.Errorf("Errors with the same code and message should be equal")
	}

	// same error codes
	err2 = New(ERR_NOT_FOUND, "invalid argument")

	if !err1.Is(err2) {
		t.Errorf("Errors with same codes should be equal")
	}

	// different error codes
	err2 = New(ERR_INVALID_ARGUMENT, "resource not found")
	if err1.Is(err2) {
		t.Errorf("Errors with different codes should not be equal")
	}

	dsErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "[ValidateBlock][%s] failed to set block subtrees_set: ")
	require.True(t, dsErr.Is(ErrTxInvalidDoubleSpend))
}

func TestUnwrapGRPC_DifferentErrors(t *testing.T) {
	// Define test cases
	tests := []struct {
		name         string
		grpcError    error
		expectedCode ERR
		expectedMsg  string
	}{
		{
			name:         "NotFound with details",
			grpcError:    createGRPCError(ERR_NOT_FOUND, "not found detail"),
			expectedCode: ERR_NOT_FOUND,
			expectedMsg:  "not found detail",
		},
		{
			name:         "Invalid tx with details",
			grpcError:    createGRPCError(ERR_TX_INVALID_DOUBLE_SPEND, "double spend detail"),
			expectedCode: ERR_TX_INVALID_DOUBLE_SPEND,
			expectedMsg:  "double spend detail",
		},
		{
			name:         "Invalid block with details",
			grpcError:    createGRPCError(ERR_BLOCK_INVALID, "invalid block detail"),
			expectedCode: ERR_BLOCK_INVALID,
			expectedMsg:  "invalid block detail",
		},
		{
			name:         "InvalidArgument without details",
			grpcError:    status.Error(codes.InvalidArgument, "invalid argument"),
			expectedCode: ErrInvalidArgument.Code,
			expectedMsg:  "invalid argument",
		},
		{
			name:         "Unknown error",
			grpcError:    status.Error(codes.Unknown, "unknown error"),
			expectedCode: ErrUnknown.Code,
			expectedMsg:  "unknown error",
		},
	}

	// Run test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			unwrappedErr := UnwrapGRPC(tc.grpcError)
			uErr, ok := unwrappedErr.(*Error)
			if !ok {
				t.Fatalf("expected *Error type; got %T", unwrappedErr)
			}

			if uErr.Code != tc.expectedCode {
				t.Errorf("expected code %v; got %v", tc.expectedCode, uErr.Code)
			}

			if uErr.Message != tc.expectedMsg {
				t.Errorf("expected message %q; got %q", tc.expectedMsg, uErr.Message)
			}
		})
	}
}

func Test_UnwrapChain(t *testing.T) {
	baseErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "base error")
	wrappedOnce := fmt.Errorf("error wrapped once: %w", baseErr)
	wrappedTwice := fmt.Errorf("error wrapped twice: %w", wrappedOnce)

	if !errors.Is(wrappedTwice, baseErr) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}

	if !errors.Is(wrappedTwice, wrappedOnce) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}
}

func Test_GRPCErrorsRoundTrip(t *testing.T) {
	originalErr := New(ERR_BLOCK_NOT_FOUND, "not found")
	wrappedGRPCError := WrapGRPC(originalErr)
	unwrappedError := UnwrapGRPC(wrappedGRPCError)

	if !errors.Is(unwrappedError, originalErr) {
		t.Errorf("Unwrapped error does not match original error after gRPC round trip")
	}
}

// Helper function to create a gRPC error with UBSVError details
func createGRPCError(code ERR, msg string) error {
	grpcCode := ErrorCodeToGRPCCode(code)
	detail := &TError{
		Code:    code,
		Message: msg,
	}
	anyDetail, err := anypb.New(detail)
	if err != nil {
		panic("failed to create anypb.Any from UBSVError")
	}
	st := status.New(grpcCode, "error with details")
	st, err = st.WithDetails(anyDetail)
	if err != nil {
		panic("failed to add details to status")
	}
	return st.Err()
}

func TestStorageError(t *testing.T) {
	otherError := New(ERR_ERROR, "storage error")
	require.NotNil(t, otherError)
	require.Equal(t, ERR_ERROR, otherError.Code)

	err := New(ERR_STORAGE_ERROR, "error getting transaction %s from txMetaStore", otherError).WithData("block id", chainhash.Hash{})
	require.NotNil(t, err)
	require.Equal(t, ERR_STORAGE_ERROR, err.Code)
	require.True(t, Is(err, ErrStorageError), "expected error to be of type ERR_STORAGE_ERROR")
	require.True(t, err.Is(otherError), "expected error to be of type otherError")
}

func Test_JoinWithMultipleErrs(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "not found")
	err2 := New(ERR_BLOCK_NOT_FOUND, "block not found")
	err3 := New(ERR_INVALID_ARGUMENT, "invalid argument")

	joinedErr := Join(err1, err2, err3)
	require.NotNil(t, joinedErr)
	require.Equal(t, "Error: NOT_FOUND (error code: 3),  not found: <nil>, Error: BLOCK_NOT_FOUND (error code: 10),  block not found: <nil>, Error: INVALID_ARGUMENT (error code: 1),  invalid argument: <nil>", joinedErr.Error())
}

func Test_AddDataToWrappedError(t *testing.T) {
	// Create a base error
	baseErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "base error")

	// Wrap the base error multiple times
	wrappedOnce := baseErr
	wrappedTwice := New(ERR_UNKNOWN, "error wrapped twice", wrappedOnce)

	// Add data to the twice wrapped error
	data := map[string]interface{}{
		"key": 1000,
	}
	errWithData := wrappedTwice.WithData("key", 1000)

	// Check if the data is added correctly
	if !dataEqual(errWithData.Data, data) {
		t.Errorf("Failed to add data to the wrapped error")
	}

	// Check if the base error is still reachable through unwrapping
	if !errors.Is(errWithData, baseErr) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}
}

func Test_AddDataToWrappedError2(t *testing.T) {
	// Create a base error
	baseErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "base error").WithData("key0", 0)

	// Wrap the base error multiple times, each time adding additional data
	wrappedOnce := baseErr.WithData("key1", 1000)
	wrappedTwice := New(ERR_UNKNOWN, "error wrapped twice", wrappedOnce).WithData("key2", 2000)
	wrappedThrice := New(ERR_BLOCK_NOT_FOUND, "error wrapped thrice", wrappedTwice).WithData("key3", 3000)

	// wrappedThrice should have the data from all the wrapping errors, we can access by unwrapping
	// fmt.Println("Error data:", wrappedThrice.Data)
	require.True(t, dataEqual(wrappedThrice.Data, map[string]interface{}{"key0": 0, "key1": 1000, "key2": 2000, "key3": 3000}))

	// Check if the base error is still reachable through unwrapping
	if !errors.Is(wrappedThrice, baseErr) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}
}

// dataEqual checks if two maps are equal
func dataEqual(map1, map2 map[string]interface{}) bool {
	if len(map1) != len(map2) {
		//fmt.Println("Length mismatch")
		return false
	}
	for k, v := range map1 {
		if val, ok := map2[k]; !ok || val != v {
			fmt.Println("Mismatch in key:", k, ", val1:", v, ", val2:", val)
			return false
		}
	}
	return true
}
