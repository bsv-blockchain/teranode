package errors

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// Test_NewCustomError tests the creation of custom errors.
func Test_NewCustomError(t *testing.T) {
	err := New(ERR_NOT_FOUND, "resource not found")
	assert.NotNil(t, err)
	assert.Equal(t, ERR_NOT_FOUND, err.Code)
	assert.Equal(t, "resource not found", err.Message)

	secondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", err)
	thirdErr := New(ERR_INVALID_TX_DOUBLE_SPEND, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_test_string_", secondErr)
	anotherErr := New(ERR_INVALID_TX_DOUBLE_SPEND, "Another ERR, block is invalid")
	olderError := fmt.Errorf("older error: %w", thirdErr)
	fourthErr := New(ERR_INVALID_BLOCK, "invalid tx double spend error", olderError)

	assert.ErrorIs(t, anotherErr, thirdErr)
	assert.ErrorIs(t, fourthErr, ErrInvalidTxDoubleSpend)
	assert.ErrorIs(t, fourthErr, err)
	assert.NotErrorIs(t, anotherErr, fourthErr)
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

	// if s.Message() != "not found" {
	// 	t.Errorf("expected gRPC message 'not found'; got '%s'", s.Message())
	// }
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

	err = New(ERR_INVALID_BLOCK, "invalid block error")
	if !errors.Is(err, New(ERR_INVALID_BLOCK, "")) {
		t.Errorf("errors.Is failed to recognize INVALID_BLOCK error type")
	}

	err = New(ERR_INVALID_TX_DOUBLE_SPEND, "invalid tx double spend error")
	if !errors.Is(err, New(ERR_INVALID_TX_DOUBLE_SPEND, "")) {
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
	originalErr := New(ERR_INVALID_TX_DOUBLE_SPEND, "original error")
	wrappedErr := New(ERR_INVALID_BLOCK, "Some more additional context", originalErr)

	if !errors.Is(wrappedErr, originalErr) {
		t.Errorf("Wrapped error does not match original error")
	}

	if !strings.Contains(wrappedErr.Error(), "Some more additional context") {
		t.Errorf("Wrapped error does not contain additional context")
	}

	fmt.Println(wrappedErr)
}

func Test_ErrorEquality(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "resource not found")
	err2 := New(ERR_NOT_FOUND, "resource not found")

	if !errors.Is(err1, err2) {
		t.Errorf("Errors with the same code and message should be equal")
	}

	// same error codes
	err2 = New(ERR_NOT_FOUND, "invalid argument")

	if !errors.Is(err1, err2) {
		t.Errorf("Errors with same codes should be equal")
	}

	// different error codes
	err2 = New(ERR_INVALID_ARGUMENT, "resource not found")
	if errors.Is(err1, err2) {
		t.Errorf("Errors with different codes should not be equal")
	}
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
			grpcError:    createGRPCError(ERR_INVALID_TX_DOUBLE_SPEND, "double spend detail"),
			expectedCode: ERR_INVALID_TX_DOUBLE_SPEND,
			expectedMsg:  "double spend detail",
		},
		{
			name:         "Invalid block with details",
			grpcError:    createGRPCError(ERR_INVALID_BLOCK, "invalid block detail"),
			expectedCode: ERR_INVALID_BLOCK,
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
	baseErr := New(ERR_INVALID_TX_DOUBLE_SPEND, "base error")
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
	detail := &UBSVError{
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
