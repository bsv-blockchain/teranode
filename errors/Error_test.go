package errors

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
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

// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
// TestUnwrapGRPC tests unwrapping a gRPC error back to a custom error.
func Test_WrapUnwrapGRPC(t *testing.T) {
	err := NewNotFoundError("not found")
	wrappedErr := WrapGRPC(err)
	// Unwrap
	unwrappedErr := UnwrapGRPC(wrappedErr)

	// Check error properties
	require.True(t, unwrappedErr.Is(err))
}

func Test_ErrorIs(t *testing.T) {
	err := New(ERR_NOT_FOUND, "not found")
	require.True(t, err.Is(ErrNotFound))

	err = New(ERR_BLOCK_INVALID, "invalid block error")
	require.True(t, err.Is(ErrBlockInvalid))
}

func ReturnsError() error {
	return NewTxNotFoundError("Tx not found")
}

func Test_Errors_Standard_Is(t *testing.T) {
	err := ReturnsError()
	txNotFoundError := NewTxNotFoundError("Tx not found")

	// fmt.Println("Return error:", err)
	// fmt.Println("Actual error:", txNotFoundError)

	require.True(t, Is(err, txNotFoundError))
	require.True(t, Is(err, ErrTxNotFound))

	fmtError := fmt.Errorf("can't query aerospike")
	serviceError := NewServiceError("Aerospike service error", fmtError)
	require.True(t, Is(serviceError, fmtError))
	require.True(t, serviceError.Is(fmtError))
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
		grpcError    *Error
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
	}

	// Run test cases
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			unwrappedErr := UnwrapGRPC(tc.grpcError)

			if unwrappedErr.Code != tc.expectedCode {
				t.Errorf("expected code %v; got %v", tc.expectedCode, unwrappedErr.Code)
			}

			if unwrappedErr.Message != tc.expectedMsg {
				t.Errorf("expected message %q; got %q", tc.expectedMsg, unwrappedErr.Message)
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

// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
func Test_GRPCErrorsRoundTrip(t *testing.T) {
	originalErr := New(ERR_BLOCK_NOT_FOUND, "not found")
	wrappedGRPCError := WrapGRPC(originalErr)
	unwrappedError := UnwrapGRPC(wrappedGRPCError)

	require.True(t, unwrappedError.Is(originalErr))
}

// Helper function to create a gRPC error with UBSVError details
func createGRPCError(code ERR, msg string) *Error {
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

	return &Error{
		Code:       code,
		Message:    msg,
		WrappedErr: st.Err(),
	}
}

func Test_UtxoSpentError(t *testing.T) {
	baseErr := New(ERR_ERROR, "storage error")
	require.NotNil(t, baseErr)
	require.Equal(t, ERR_ERROR, baseErr.Code)

	utxoSpentError := NewUtxoSpentErr(chainhash.Hash{}, chainhash.Hash{}, time.Now(), baseErr)
	require.NotNil(t, utxoSpentError)
	require.True(t, utxoSpentError.Is(ErrTxAlreadyExists), "expected error to be of type ERR_TX_ALREADY_EXISTS")
	require.True(t, utxoSpentError.Is(baseErr), "expected error to be of type baseErr")
	require.False(t, utxoSpentError.Is(ErrBlockInvalid), "expected error to be of type baseErr")

	var spentErr *UtxoSpentErrData
	require.True(t, utxoSpentError.As(&spentErr))

	// wrap utxospent
	wrappedUtxoSpent := New(ERR_BLOCK_INVALID, "utxoSpentErrStruct.Error()", utxoSpentError)
	require.True(t, wrappedUtxoSpent.As(&spentErr))

	anotherErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "Another ERR, block is invalid")
	require.False(t, anotherErr.As(&spentErr))
}

func Test_JoinWithMultipleErrs(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "not found")
	err2 := New(ERR_BLOCK_NOT_FOUND, "block not found")
	err3 := New(ERR_INVALID_ARGUMENT, "invalid argument")

	joinedErr := Join(err1, err2, err3)
	require.NotNil(t, joinedErr)
	require.Equal(t, "Error: NOT_FOUND, (error code: 3), Message: not found, Error: BLOCK_NOT_FOUND, (error code: 10), Message: block not found, Error: INVALID_ARGUMENT, (error code: 1), Message: invalid argument", joinedErr.Error())
}

func TestErrorString(t *testing.T) {
	err := errors.New("some error")

	thisErr := NewStorageError("failed to set data from reader [%s:%s]", "bucket", "key", err)

	assert.Equal(t, "Error: STORAGE_ERROR (error code: 59), Message: failed to set data from reader [bucket:key], Wrapped err: Error: UNKNOWN, (error code: 0), Message: some error", thisErr.Error())
}

func Test_VariousChainedErrorsWithWrapUnwrapGRPC(t *testing.T) {
	// Base error is not a GRPC error, basic error
	baseServiceErr := NewServiceError("block is invalid")
	baseServiceErrWithNew := New(ERR_SERVICE_ERROR, "block is invalid")
	require.True(t, baseServiceErrWithNew.Is(baseServiceErr))
	require.True(t, baseServiceErr.Is(baseServiceErrWithNew))

	baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	baseBlockInvalidErrWithNew := New(ERR_BLOCK_INVALID, "block is invalid")
	require.True(t, baseBlockInvalidErr.Is(baseBlockInvalidErrWithNew))

	wrappedOnce := WrapGRPC(baseServiceErr)
	unwrapped := UnwrapGRPC(wrappedOnce)

	require.True(t, baseServiceErr.Is(unwrapped))
	require.True(t, unwrapped.Is(baseServiceErr))

	// baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	fmtError := fmt.Errorf("can't query transaction meta from aerospike")
	txInvalidErr := NewTxInvalidError("tx is invalid", fmtError)
	level1BlockInvalidError := NewBlockInvalidError("block is invalid", txInvalidErr)
	level2ServiceError := NewServiceError("service error", level1BlockInvalidError)
	level3ProcessingError := NewProcessingError("processing error", level2ServiceError)
	level4ContextError := NewContextError("context error", level3ProcessingError)

	// Test errors that are nested
	// level 2 error recognizes all the errors in the chain
	require.True(t, level2ServiceError.Is(fmtError))
	require.True(t, level2ServiceError.Is(txInvalidErr))
	require.True(t, level2ServiceError.Is(baseBlockInvalidErr))
	require.True(t, level2ServiceError.Is(ErrServiceError))
	require.True(t, level2ServiceError.Is(ErrBlockInvalid))
	require.True(t, level2ServiceError.Is(ErrTxInvalid))

	// Test that we don't lose any data when wrapping and unwrapping GRPC
	wrapped := WrapGRPC(level4ContextError)
	unwrapped = UnwrapGRPC(wrapped)

	// checks with the Is function
	require.True(t, unwrapped.Is(fmtError))
	require.True(t, unwrapped.Is(txInvalidErr))
	require.True(t, unwrapped.Is(baseBlockInvalidErr))
	require.True(t, unwrapped.Is(ErrServiceError))
	require.True(t, unwrapped.Is(ErrBlockInvalid))
	require.True(t, unwrapped.Is(ErrTxInvalid))
	require.True(t, unwrapped.Is(level2ServiceError))
	require.True(t, unwrapped.Is(level3ProcessingError))
	require.True(t, unwrapped.Is(level4ContextError))

	// checks with the standard Is function
	require.True(t, errors.Is(unwrapped, fmtError))
	require.True(t, errors.Is(unwrapped, txInvalidErr))
	require.True(t, errors.Is(unwrapped, baseBlockInvalidErr))
	require.True(t, errors.Is(unwrapped, ErrServiceError))
	require.True(t, errors.Is(unwrapped, ErrBlockInvalid))
	require.True(t, errors.Is(unwrapped, ErrTxInvalid))
	require.True(t, errors.Is(unwrapped, level2ServiceError))
	require.True(t, errors.Is(unwrapped, level3ProcessingError))
	require.True(t, errors.Is(unwrapped, level4ContextError))
}

func ReturnErrorAsStandardErrorWithoutModification(error *Error) error {
	return error
}

func ReturnSpecialErrorFromStandardErrorWithModification(error error) *Error {
	return NewError("error on the top", error)
}

// An error scenario, from top to down:
// handle_block.go: errors.NewProcessingError("failed to process block", err)
// in blockvalidation/Client.go: errors.UnwrapGRPC(err)
// blockvalidation/Server.go: ProcessBlock returns error, Let's assume it is an error returned by blockchain service: 	exists, err := u.blockchainClient.GetBlockExists(ctx, hash)
//	if err != nil {
//		return false, err
//	}
// 	blockvalidation/Server.go:  processBlockFound returns: parentExists, err := u.blockValidation.GetBlockExists(ctx, block.Header.HashPrevBlock)
//	if err != nil {
//		return errors.WrapGRPC(
//			errors.NewServiceError("[processBlockFound][%s] failed to check if parent block %s exists", hash.String(), block.Header.HashPrevBlock.String(), err))
//	}
//  blockhain/Client.go:  unwrap
// blockchain/Server.go: 	exists, err := b.store.GetBlockExists(ctx, blockHash)
//	if err != nil {
//		return nil, errors.WrapGRPC(err)
//	}
// store/sql/GetBlockExists.go: error is returned from Query Row Content

func Test_WrapUnwrapMissingDetailsErr(t *testing.T) {
	blockHash := chainhash.Hash{'1', '2', '3', '4'}

	// SCENARIO 2

	// blockvalidation/Server.go: replicate error in ValidateBlock
	blockInvalidError := NewBlockInvalidError("[ValidateBlock][%s] block size %d exceeds excessiveblocksize %d", blockHash.String(), 120, 100)
	// blockvalidation/Server.go: replicate error in processBlockFound returned exactly as it is by ProcessBlock
	wrappedBlockInvalidError := WrapGRPC(NewServiceError("failed block validation BlockFound [%s]", blockHash.String(), blockInvalidError))
	// blockvalidation/Client.go: unwrap
	unwrappedBlockInvalidError := UnwrapGRPC(wrappedBlockInvalidError)
	// replicate handle_block.go:
	processingError := NewProcessingError("failed to process block", unwrappedBlockInvalidError)

	fmt.Println("Scenario 1 error:\n", processingError)

	// SCENARIO 2
	// replicate store/sql/GetBlockExists.go
	sqlGetBlockExistsError := fmt.Errorf("sql: expected %d arguments, got %d", 5, 3)
	// replicate blockchain/Server.go: wrap
	wrappedErrorSQLGetBlockExistsError := WrapGRPC(sqlGetBlockExistsError)
	// replicate blockchain/Client.go: unwrap
	unwrappedErrorSQLGetBlockExistsError := UnwrapGRPC(wrappedErrorSQLGetBlockExistsError)
	// blockvalidation/Server.go: processBlockFound creates service error and wraps error,
	serviceError := NewServiceError("[processBlockFound][%s] failed to check if parent block %s exists", blockHash.String(), blockHash.String(), unwrappedErrorSQLGetBlockExistsError)
	wrappedServiceError := WrapGRPC(serviceError)
	// fmt.Println("Wrapped error:\n", wrappedServiceError)
	// wrapTwice := WrapGRPC(wrappedServiceError)
	// fmt.Println("Wrapped error twice:\n", wrapTwice)
	// replicate blockvalidation/Client.go: unwrap
	unwrappedServiceError := UnwrapGRPC(wrappedServiceError)
	// replicate handle_block.go:
	processingError = NewProcessingError("failed to process block", unwrappedServiceError)
	fmt.Println("Scenario 2 error:\n", processingError)
}

func Test_VariousChainedErrorsConvertedToStandardErrorWithWrapUnwrapGRPC(t *testing.T) {
	// Base error is not a GRPC error, basic error
	baseServiceErr := NewServiceError("block is invalid")
	baseServiceErrWithNew := New(ERR_SERVICE_ERROR, "block is invalid")
	require.True(t, baseServiceErrWithNew.Is(baseServiceErr))
	require.True(t, baseServiceErr.Is(baseServiceErrWithNew))

	standardizedBaseError := ReturnErrorAsStandardErrorWithoutModification(baseServiceErr)

	wrappedOnce := WrapGRPC(standardizedBaseError)
	unwrapped := UnwrapGRPC(wrappedOnce)

	require.True(t, baseServiceErr.Is(unwrapped))
	require.True(t, unwrapped.Is(baseServiceErr))

	baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	baseBlockInvalidErrWithNew := New(ERR_BLOCK_INVALID, "block is invalid")
	require.True(t, baseBlockInvalidErr.Is(baseBlockInvalidErrWithNew))

	// baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	fmtError := fmt.Errorf("can't query transaction meta from aerospike")
	txInvalidErr := NewTxInvalidError("tx is invalid", fmtError)
	level1BlockInvalidError := NewBlockInvalidError("block is invalid", txInvalidErr)
	level2ServiceError := NewServiceError("service error", level1BlockInvalidError)
	level3ProcessingError := NewProcessingError("processing error", level2ServiceError)
	level4ContextError := NewContextError("context error", level3ProcessingError)

	// Test errors that are nested
	// level 2 error recognizes all the errors in the chain
	require.True(t, level2ServiceError.Is(fmtError))
	require.True(t, level2ServiceError.Is(txInvalidErr))
	require.True(t, level2ServiceError.Is(baseBlockInvalidErr))
	require.True(t, level2ServiceError.Is(ErrServiceError))
	require.True(t, level2ServiceError.Is(ErrBlockInvalid))
	require.True(t, level2ServiceError.Is(ErrTxInvalid))

	standardizedComplexError := ReturnErrorAsStandardErrorWithoutModification(level4ContextError)
	require.True(t, Is(standardizedComplexError, level4ContextError))
	require.True(t, Is(standardizedComplexError, level3ProcessingError))
	require.True(t, Is(standardizedComplexError, level2ServiceError))
	require.True(t, Is(standardizedComplexError, level1BlockInvalidError))
	require.True(t, Is(standardizedComplexError, txInvalidErr))

	// Test that we don't lose any data when wrapping and unwrapping GRPC
	topError := ReturnSpecialErrorFromStandardErrorWithModification(standardizedComplexError)
	// fmt.Println("\nStandardized and then Wrapped error:\n", topError)
	require.True(t, Is(topError, level4ContextError))
	require.True(t, Is(topError, level3ProcessingError))
	require.True(t, Is(topError, level2ServiceError))
	require.True(t, Is(topError, level1BlockInvalidError))
	require.True(t, Is(topError, txInvalidErr))

	wrapped := WrapGRPC(topError)
	// fmt.Println("\nWrapped error:\n", topError)
	unwrapped = UnwrapGRPC(wrapped)
	// fmt.Println("\nUnwrapped error:\n", unwrapped)

	// checks with the Is function
	require.True(t, unwrapped.Is(fmtError))
	require.True(t, unwrapped.Is(txInvalidErr))
	require.True(t, unwrapped.Is(baseBlockInvalidErr))
	require.True(t, unwrapped.Is(ErrServiceError))
	require.True(t, unwrapped.Is(ErrBlockInvalid))
	require.True(t, unwrapped.Is(ErrTxInvalid))
	require.True(t, unwrapped.Is(level2ServiceError))
	require.True(t, unwrapped.Is(level3ProcessingError))
	require.True(t, unwrapped.Is(level4ContextError))

	// checks with the standard Is function
	require.True(t, errors.Is(unwrapped, fmtError))
	require.True(t, errors.Is(unwrapped, txInvalidErr))
	require.True(t, errors.Is(unwrapped, baseBlockInvalidErr))
	require.True(t, errors.Is(unwrapped, ErrServiceError))
	require.True(t, errors.Is(unwrapped, ErrBlockInvalid))
	require.True(t, errors.Is(unwrapped, ErrTxInvalid))
	require.True(t, errors.Is(unwrapped, level2ServiceError))
	require.True(t, errors.Is(unwrapped, level3ProcessingError))
	require.True(t, errors.Is(unwrapped, level4ContextError))
}

func Test_UnwrapGRPCWithStandardError(t *testing.T) {
	// Create a simple gRPC error with a standard error message
	grpcErr := status.Error(codes.InvalidArgument, "Invalid argument provided")

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrapped := UnwrapGRPC(grpcErr)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrapped)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrapped.Code)
	require.Equal(t, "rpc error: code = InvalidArgument desc = Invalid argument provided", unwrapped.Message)

	// Test with a different gRPC status code
	grpcErrNotFound := status.Error(codes.NotFound, "Resource not found")

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrappedNotFound := UnwrapGRPC(grpcErrNotFound)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrappedNotFound)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrappedNotFound.Code)
	require.Equal(t, "rpc error: code = NotFound desc = Resource not found", unwrappedNotFound.Message)
}

func Test_UnwrapGRPCWithStandardKENError(t *testing.T) {
	// Create a standard error
	standardErr := fmt.Errorf("invalid argument provided")
	grpcErr := status.Error(codes.InvalidArgument, standardErr.Error())

	// Wrap the standard error using WrapGRPC
	wrappedErr := WrapGRPC(grpcErr)
	// fmt.Println("WrapGRPC returned: ", wrappedErr)

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrapped := UnwrapGRPC(wrappedErr)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrapped)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrapped.Code)
	require.Equal(t, "rpc error: code = InvalidArgument desc = invalid argument provided", unwrapped.Message)

	// Test with a different gRPC status code and message
	standardErrResourceExhausted := fmt.Errorf("Resource exhausted")
	grpcErr = status.Error(codes.ResourceExhausted, standardErrResourceExhausted.Error())
	wrappedErrResourceExhausted := WrapGRPC(grpcErr)

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrappedResourceExhausted := UnwrapGRPC(wrappedErrResourceExhausted)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrappedResourceExhausted)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrappedResourceExhausted.Code)
	require.Equal(t, "rpc error: code = ResourceExhausted desc = Resource exhausted", unwrappedResourceExhausted.Message)
}
