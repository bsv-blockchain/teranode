// nolint:forbidigo,depguard // This test file needs the standard errors package for testing the custom errors package
package errors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/bitcoin-sv/teranode/errors/grpctest/github.com/bitcoin-sv/ubsv/errors/grpctest"
	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
)

// TestNewCustomError tests the creation of custom errors.
func TestNewCustomError(t *testing.T) {
	err := New(ERR_NOT_FOUND, "resource not found")
	require.NotNil(t, err)
	require.Equal(t, ERR_NOT_FOUND, err.code)
	require.Equal(t, "resource not found", err.message)

	secondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_teststring_", err)
	thirdErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_teststring_", secondErr)
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

// TestFmtErrorCustomError tests formatting a custom error with fmt.Errorf.
func TestFmtErrorCustomError(t *testing.T) {
	err := New(ERR_NOT_FOUND, "resource not found")
	require.NotNil(t, err)
	require.Equal(t, ERR_NOT_FOUND, err.code)
	require.Equal(t, "resource not found", err.message)

	fmtError := fmt.Errorf("error: %w", err)
	require.NotNil(t, fmtError)
	secondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_teststring_", fmtError)
	require.NotNil(t, secondErr)

	// If we FMT Err, then they won't be recognized as equal
	require.False(t, secondErr.Is(err))

	altErr := New(ERR_INVALID_ARGUMENT, "invalid argument", err)
	altSecondErr := New(ERR_INVALID_ARGUMENT, "[ValidateBlock][%s] failed to set block subtrees_set: ", "_teststring_", fmtError)
	require.True(t, altSecondErr.Is(altErr))
}

// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
// TestUnwrapGRPC tests unwrapping a gRPC error back to a custom error.
func TestWrapUnwrapGRPC(t *testing.T) {
	err := NewNotFoundError("not found")
	wrappedErr := WrapGRPC(err)
	// Unwrap
	unwrappedErr := UnwrapGRPC(wrappedErr)

	// Check error properties
	require.True(t, unwrappedErr.Is(err))
}

// TestErrorIs tests the Is method of the custom error.
func TestErrorIs(t *testing.T) {
	err := New(ERR_NOT_FOUND, "not found")
	require.True(t, err.Is(ErrNotFound))

	err = New(ERR_BLOCK_INVALID, "invalid block error")
	require.True(t, err.Is(ErrBlockInvalid))
}

// ReturnsError returns a custom error for testing purposes.
func ReturnsError() error {
	return NewTxNotFoundError("Tx not found")
}

// TestErrors_Standard_Is tests the Is method of the custom error against standard errors.
func TestErrors_Standard_Is(t *testing.T) {
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

// TestErrorWrapWithAdditionalContext tests wrapping an error with additional context.
func TestErrorWrapWithAdditionalContext(t *testing.T) {
	originalErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "original error")
	wrappedErr := New(ERR_BLOCK_INVALID, "Some more additional context", originalErr)

	if !Is(wrappedErr, originalErr) {
		t.Errorf("Wrapped error does not match original error")
	}

	if !strings.Contains(wrappedErr.Error(), "Some more additional context") {
		t.Errorf("Wrapped error does not contain additional context")
	}
}

// TestErrorEquality tests the equality of custom errors.
func TestErrorEquality(t *testing.T) {
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

// TestUnwrapGRPC_DifferentErrors tests unwrapping gRPC errors with different error codes and messages.
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

			if unwrappedErr.Code() != tc.expectedCode {
				t.Errorf("expected code %v; got %v", tc.expectedCode, unwrappedErr.Code())
			}

			if unwrappedErr.Message() != tc.expectedMsg {
				t.Errorf("expected message %q; got %q", tc.expectedMsg, unwrappedErr.Message())
			}
		})
	}
}

// TestUnwrapChain tests that the Is function can identify errors in the unwrapped chain.
func TestUnwrapChain(t *testing.T) {
	baseErr := New(ERR_TX_INVALID_DOUBLE_SPEND, "base error")
	wrappedOnce := fmt.Errorf("error wrapped once: %w", baseErr)
	wrappedTwice := fmt.Errorf("error wrapped twice: %w", wrappedOnce)

	if !Is(wrappedTwice, baseErr) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}

	if !Is(wrappedTwice, wrappedOnce) {
		t.Errorf("Should identify base error anywhere in the unwrap chain")
	}
}

// TODO: Put this back in when we fix WrapGRPC/UnwrapGRPC
// TestGRPCErrorsRoundTrip tests that gRPC errors can be wrapped and unwrapped correctly.
func TestGRPCErrorsRoundTrip(t *testing.T) {
	originalErr := New(ERR_BLOCK_NOT_FOUND, "not found")
	wrappedGRPCError := WrapGRPC(originalErr)
	unwrappedError := UnwrapGRPC(wrappedGRPCError)

	require.True(t, unwrappedError.Is(originalErr))
}

// Helper function to create a gRPC error with TERANODEError details
func createGRPCError(code ERR, msg string) *Error {
	grpcCode := ErrorCodeToGRPCCode(code)
	detail := &TError{
		Code:    code,
		Message: msg,
	}

	anyDetail, err := anypb.New(detail)
	if err != nil {
		panic("failed to create anypb.Any from TERANODEError")
	}

	st := status.New(grpcCode, "error with details")

	st, err = st.WithDetails(anyDetail)
	if err != nil {
		panic("failed to add details to status")
	}

	return &Error{
		code:       code,
		message:    msg,
		wrappedErr: st.Err(),
	}
}

// Test_UtxoSpentError tests the creation and properties of a UtxoSpentError.
func Test_UtxoSpentError(t *testing.T) {
	t.Run("UtxoSpentError", func(t *testing.T) {
		txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}

		spendingData := spendpkg.NewSpendingData(&txID, 1)

		utxoSpentError := NewUtxoSpentError(txID, 1, chainhash.Hash{}, spendingData)
		require.NotNil(t, utxoSpentError)
		require.True(t, utxoSpentError.Is(ErrSpent), "expected error to be of type ERR_SPENT")
		require.False(t, utxoSpentError.Is(ErrBlockInvalid), "expected error to be of type baseErr")

		var spentErr *Error

		require.True(t, utxoSpentError.As(&spentErr))

		// check the data in the error
		require.NotNil(t, spentErr.data)

		// check hash
		assert.Equal(t, txID, spentErr.data.(*UtxoSpentErrData).Hash)

		hash := spentErr.GetData("hash")
		assert.Equal(t, txID, hash)

		// check spending tx hash
		assert.Equal(t, txID, *spentErr.data.(*UtxoSpentErrData).SpendingData.TxID)

		spendingDataIfc := spentErr.GetData("spending_data")
		spendingData2, ok := spendingDataIfc.(*spendpkg.SpendingData)
		require.True(t, ok)

		assert.Equal(t, txID, *spendingData2.TxID)
		assert.Equal(t, 1, spendingData2.Vin)
	})

	t.Run("UtxoSpentErrorData grpc", func(t *testing.T) {
		txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}

		spendingData := spendpkg.NewSpendingData(&txID, 1)

		utxoSpentError := NewUtxoSpentError(txID, 1, chainhash.Hash{}, spendingData)

		// wrap the error in a gRPC error
		grpcErr := WrapGRPC(utxoSpentError)
		require.NotNil(t, grpcErr)

		// unwrap the gRPC error
		unwrappedErr := UnwrapGRPC(grpcErr)
		require.NotNil(t, unwrappedErr)

		// check unwrapped error is the same as the original error
		require.True(t, unwrappedErr.Is(utxoSpentError))

		// TODO wrapped error does not persist the data

		// check the data in the error
		require.NotNil(t, unwrappedErr.Data())

		var err *Error

		require.True(t, unwrappedErr.As(&err))

		// check hash
		assert.Equal(t, txID, unwrappedErr.Data().(*UtxoSpentErrData).Hash)

		hash := unwrappedErr.Data().GetData("hash")
		assert.Equal(t, txID, hash)

		// check spending tx hash
		assert.Equal(t, txID, *unwrappedErr.Data().(*UtxoSpentErrData).SpendingData.TxID)

		spendingDataIfc := unwrappedErr.Data().GetData("spending_data")
		spendingData2, ok := spendingDataIfc.(*spendpkg.SpendingData)
		require.True(t, ok)

		assert.Equal(t, txID, *spendingData2.TxID)
		assert.Equal(t, 1, spendingData2.Vin)

		contains := "70: UTXO_SPENT (70): 0000000000000000000000000000000000000000000000313233343536373839:1 utxo already spent by tx 0000000000000000000000000000000000000000000000313233343536373839[1] \"utxo 0000000000000000000000000000000000000000000000313233343536373839 already spent by 0000000000000000000000000000000000000000000000313233343536373839[1]\""

		assert.Contains(t, unwrappedErr.Error(), contains)
	})

	t.Run("Set data for invalid block error", func(t *testing.T) {
		invalidBlockError := NewBlockInvalidError("block is invalid")
		require.NotNil(t, invalidBlockError)
		require.True(t, invalidBlockError.Is(ErrBlockInvalid))

		invalidBlockError.SetData("key", "value")
		data := invalidBlockError.GetData("key")
		assert.Equal(t, "value", data)

		wrappedErr := WrapGRPC(invalidBlockError)
		require.NotNil(t, wrappedErr)

		unwrappedErr := UnwrapGRPC(wrappedErr)
		require.NotNil(t, unwrappedErr)

		require.True(t, unwrappedErr.Is(invalidBlockError))

		// check the data in the error
		require.NotNil(t, unwrappedErr.Data())
		require.Equal(t, "value", unwrappedErr.Data().GetData("key"))
	})
}

// TestJoinWithMultipleErrs tests the Join function with multiple errors.
func TestJoinWithMultipleErrs(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "not found")
	err2 := New(ERR_BLOCK_NOT_FOUND, "block not found")
	err3 := New(ERR_INVALID_ARGUMENT, "invalid argument")

	joinedErr := Join(err1, err2, err3)
	require.NotNil(t, joinedErr)
	assert.Equal(t, "NOT_FOUND (3): not found, BLOCK_NOT_FOUND (10): block not found, INVALID_ARGUMENT (1): invalid argument", joinedErr.Error())
}

// TestErrorString tests the string representation of a custom error.
func TestErrorString(t *testing.T) {
	err := errors.New("some error")

	thisErr := NewStorageError("failed to set data from reader [%s:%s]", "bucket", "key", err)

	assert.Equal(t, "STORAGE_ERROR (69): failed to set data from reader [bucket:key] -> UNKNOWN (0): some error", thisErr.Error())
}

// TestVariousChainedErrorsWithWrapUnwrapGRPC tests various chained errors with wrapping and unwrapping using gRPC.
func TestVariousChainedErrorsWithWrapUnwrapGRPC(t *testing.T) {
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
	level4ContextError := NewContextCanceledError("context error", level3ProcessingError)

	// Test errors that are nested
	// level 2 error recognize all the errors in the chain
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
	require.True(t, Is(unwrapped, fmtError))
	require.True(t, Is(unwrapped, txInvalidErr))
	require.True(t, Is(unwrapped, baseBlockInvalidErr))
	require.True(t, Is(unwrapped, ErrServiceError))
	require.True(t, Is(unwrapped, ErrBlockInvalid))
	require.True(t, Is(unwrapped, ErrTxInvalid))
	require.True(t, Is(unwrapped, level2ServiceError))
	require.True(t, Is(unwrapped, level3ProcessingError))
	require.True(t, Is(unwrapped, level4ContextError))
}

// ReturnErrorAsStandardErrorWithoutModification returns the error without any modification.
func ReturnErrorAsStandardErrorWithoutModification(error *Error) error {
	return error
}

// ReturnSpecialErrorFromStandardErrorWithModification returns a new error based on the provided standard error, with some modification.
func ReturnSpecialErrorFromStandardErrorWithModification(error error) *Error {
	return NewError("error on the top", error)
}

// Test_WrapUnwrapMissingDetailsErr tests the wrapping and unwrapping of errors that are missing details.
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
	require.True(t, processingError.Is(blockInvalidError))
	// fmt.Println("Scenario 1 error:", processingError)

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
	require.NotNil(t, processingError)
	// fmt.Println("Scenario 2 error:\n", processingError)
}

// Test_UtxoSpentErrorUnwrapWrapWithMockGRPCServer tests the wrapping and unwrapping of a UtxoSpentError using a mock gRPC server.
func Test_UtxoSpentErrorUnwrapWrapWithMockGRPCServer(t *testing.T) {
	// Set up the server
	lis, err := net.Listen("tcp", "localhost:0") // Use port 0 for an available port
	require.NoError(t, err)

	serverAddr := lis.Addr().String()

	// create a gRPC server and register the service
	grpcServer := grpc.NewServer()
	grpctest.RegisterTestServiceServer(grpcServer, &server{})

	go func() {
		err := grpcServer.Serve(lis)
		require.NoError(t, err)
	}()

	defer grpcServer.Stop()

	// Allow some time for the server to start
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := grpc.NewClient("dns:///"+serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer func() {
		_ = clientConn.Close()
	}()

	// Use the client connection (e.g., to make a gRPC request)
	client := grpctest.NewTestServiceClient(clientConn)

	// Make the gRPC call
	req := &grpctest.TestRequest{
		Message: "Hello",
	}
	_, err = client.TestMethod(ctx, req)
	require.Error(t, err)

	unwrappedUtxoSpentError := UnwrapGRPC(err)
	require.NotNil(t, unwrappedUtxoSpentError)
}

// Test_VariousChainedErrorsConvertedToStandardErrorWithWrapUnwrapGRPC tests various chained errors converted to standard error with wrapping and unwrapping using gRPC.
func Test_VariousChainedErrorsConvertedToStandardErrorWithWrapUnwrapGRPC(t *testing.T) {
	// Base error is not a GRPC error, basic error
	baseServiceErr := NewServiceError("block is invalid")
	baseServiceErrWithNew := New(ERR_SERVICE_ERROR, "block is invalid")
	require.True(t, baseServiceErrWithNew.Is(baseServiceErr))
	require.True(t, baseServiceErr.Is(baseServiceErrWithNew))

	// standardizedBaseError := ReturnErrorAsStandardErrorWithoutModification(baseServiceErr)

	// wrappedOnce := WrapGRPC(standardizedBaseError)
	// unwrapped := UnwrapGRPC(wrappedOnce)

	// require.True(t, baseServiceErr.Is(unwrapped))
	// require.True(t, unwrapped.Is(baseServiceErr))

	baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	baseBlockInvalidErrWithNew := New(ERR_BLOCK_INVALID, "block is invalid")
	require.True(t, baseBlockInvalidErr.Is(baseBlockInvalidErrWithNew))

	// baseBlockInvalidErr := NewBlockInvalidError("block is invalid")
	fmtError := fmt.Errorf("can't query transaction meta from aerospike")
	txInvalidErr := NewTxInvalidError("tx is invalid", fmtError)
	level1BlockInvalidError := NewBlockInvalidError("block is invalid", txInvalidErr)
	level2ServiceError := NewServiceError("service error", level1BlockInvalidError)
	level3ProcessingError := NewProcessingError("processing error", level2ServiceError)
	level4ContextError := NewContextCanceledError("context error", level3ProcessingError)

	// Test errors that are nested
	// level 2 error recognize all the errors in the chain
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
	// fmt.Println("\nWrapped error:\n", wrapped)
	unwrapped := UnwrapGRPC(wrapped)
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
	require.True(t, Is(unwrapped, fmtError))
	require.True(t, Is(unwrapped, txInvalidErr))
	require.True(t, Is(unwrapped, baseBlockInvalidErr))
	require.True(t, Is(unwrapped, ErrServiceError))
	require.True(t, Is(unwrapped, ErrBlockInvalid))
	require.True(t, Is(unwrapped, ErrTxInvalid))
	require.True(t, Is(unwrapped, level2ServiceError))
	require.True(t, Is(unwrapped, level3ProcessingError))
	require.True(t, Is(unwrapped, level4ContextError))
}

// TestUnwrapGRPCWithStandardError tests unwrapping a gRPC error that has a standard error message.
func TestUnwrapGRPCWithStandardError(t *testing.T) {
	// Create a simple gRPC error with a standard error message
	grpcErr := status.Error(codes.InvalidArgument, "Invalid argument provided")

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrapped := UnwrapGRPC(grpcErr)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrapped)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrapped.Code())
	require.Equal(t, "rpc error: code = InvalidArgument desc = Invalid argument provided", unwrapped.Message())

	// Test with a different gRPC status code
	grpcErrNotFound := status.Error(codes.NotFound, "Resource not found")

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrappedNotFound := UnwrapGRPC(grpcErrNotFound)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrappedNotFound)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrappedNotFound.Code())
	require.Equal(t, "rpc error: code = NotFound desc = Resource not found", unwrappedNotFound.Message())
}

// TestUnwrapGRPCWithAnotherStandardError tests unwrapping a gRPC error that has another standard error message.
func TestUnwrapGRPCWithAnotherStandardError(t *testing.T) {
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
	require.Equal(t, ERR_ERROR, unwrapped.Code())
	require.Equal(t, "rpc error: code = InvalidArgument desc = invalid argument provided", unwrapped.Message())

	// Test with a different gRPC status code and message
	standardErrResourceExhausted := fmt.Errorf("resource exhausted")
	grpcErr = status.Error(codes.ResourceExhausted, standardErrResourceExhausted.Error())
	wrappedErrResourceExhausted := WrapGRPC(grpcErr)

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrappedResourceExhausted := UnwrapGRPC(wrappedErrResourceExhausted)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrappedResourceExhausted)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrappedResourceExhausted.Code())
	require.Equal(t, "rpc error: code = ResourceExhausted desc = resource exhausted", unwrappedResourceExhausted.Message())
}

// server is a mock gRPC server for testing purposes.
type server struct {
	grpctest.UnimplementedTestServiceServer
}

// TestMethod is a mock gRPC method that simulates an error for testing purposes.
func (s *server) TestMethod(_ context.Context, _ *grpctest.TestRequest) (*grpctest.TestResponse, error) {
	// Simulate an error
	txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}
	utxoHash := chainhash.Hash{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
	spendingTxID := chainhash.Hash{'1', '2', '3', '4', '5', '6', '7', '8', '9'}

	baseErr := NewUtxoSpentError(txID, 10, utxoHash, spendpkg.NewSpendingData(&spendingTxID, 1))
	level1Err := NewTxInvalidError("transaction invalid", baseErr)
	level2Err := NewBlockInvalidError("block invalid", level1Err)
	level3Err := NewServiceError("level service error", level2Err)
	level4Err := NewContextCanceledError("top level context error", level3Err)

	return nil, WrapGRPC(level4Err)
}

// TestWrapUnwrapGRPCWithMockGRPCServer tests wrapping and unwrapping gRPC errors with a mock gRPC server.
func TestWrapUnwrapGRPCWithMockGRPCServer(t *testing.T) {
	// Set up the server
	lis, err := net.Listen("tcp", "localhost:0") // Use port 0 for an available port
	require.NoError(t, err)

	serverAddr := lis.Addr().String()

	// create a gRPC server and register the service
	grpcServer := grpc.NewServer()
	grpctest.RegisterTestServiceServer(grpcServer, &server{})

	go func() {
		err := grpcServer.Serve(lis)
		require.NoError(t, err)
	}()

	defer grpcServer.Stop()

	// Allow some time for the server to start
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := grpc.NewClient("dns:///"+serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer func() {
		_ = clientConn.Close()
	}()

	// Use the client connection (e.g., to make a gRPC request)
	client := grpctest.NewTestServiceClient(clientConn)

	// Make the gRPC call
	req := &grpctest.TestRequest{
		Message: "Hello",
	}
	_, err = client.TestMethod(ctx, req)
	require.Error(t, err)

	// fmt.Println("\nReceived error: ", err)

	// Unwrap the error using UnwrapGRPC
	unwrappedErr := UnwrapGRPC(err)
	// fmt.Println("\nUnwrapped error: ", unwrappedErr)
	require.NotNil(t, unwrappedErr)

	var uErr *Error

	require.True(t, As(unwrappedErr, &uErr))
	require.True(t, uErr.Is(ErrServiceError))
	require.True(t, Is(unwrappedErr, ErrServiceError))
	require.True(t, uErr.Is(ErrTxInvalid))
	require.True(t, Is(unwrappedErr, ErrTxInvalid))
	require.True(t, uErr.Is(ErrBlockInvalid))
	require.True(t, Is(unwrappedErr, ErrBlockInvalid))
	require.True(t, uErr.Is(ErrContextCanceled))
	require.True(t, Is(unwrappedErr, ErrContextCanceled))
}

// TestIsErrorWithNestedErrorCodesWithWrapGRPC tests that errors with nested error codes can be identified correctly after wrapping with gRPC.
func TestIsErrorWithNestedErrorCodesWithWrapGRPC(t *testing.T) {
	errRoot := NewServiceError("service error")
	err := NewProcessingError("processing error", errRoot)
	grpcErr := WrapGRPC(err)

	require.True(t, errRoot.Is(ErrServiceError))
	require.True(t, err.Is(ErrProcessing))
	require.True(t, Is(grpcErr, ErrServiceError))
	require.True(t, Is(grpcErr, ErrProcessing))
}

// TestErrorLogging tests that errors log their messages correctly, including nested errors.
func TestErrorLogging(t *testing.T) {
	errRoot := NewServiceError("service error")
	errChild := NewStorageError("storage error", errRoot)
	err := NewProcessingError("processing error", errChild)

	sError := fmt.Sprintf("%v", err)
	require.Contains(t, sError, "service error")
	require.Contains(t, sError, "storage error")
	require.Contains(t, sError, "processing error")

	terror := Wrap(err)
	sTError := fmt.Sprintf("%v", terror)
	require.Contains(t, sTError, "service error")
	require.Contains(t, sTError, "storage error")
	require.Contains(t, sTError, "processing error")

	// fmt.Println("Error        : ", err)
	// fmt.Println("Wrapped Error: ", terror)

	require.Equal(t, sError, sTError)
}

// TestSetDataAndGetData tests the SetData and GetData methods of the Error type.
func TestSetDataAndGetData(t *testing.T) {
	err := New(ERR_BLOCK_INVALID, "block invalid")
	err.SetData("key1", "value1")
	require.Equal(t, "value1", err.GetData("key1"))

	err.SetData("key2", 12345)
	require.Equal(t, 12345, err.GetData("key2"))

	require.Nil(t, err.GetData("nonexistent"))
}

// TestRemoveInvalidUTF8 tests the RemoveInvalidUTF8 function to ensure it removes invalid UTF-8 characters.
func TestRemoveInvalidUTF8(t *testing.T) {
	input := "valid\x80invalid\x80"
	expected := "validinvalid"
	result := RemoveInvalidUTF8(input)

	require.Equal(t, expected, result)
}

// TestErrorNil tests the Error method of a nil Error pointer.
func TestErrorNil(t *testing.T) {
	var err *Error

	require.Equal(t, "<nil>", err.Error())
	require.False(t, err.Is(nil))
}

// An error that *does* implement As
type asErr struct{}

// SetData is a mock implementation of ErrDataI.SetData
func (a *asErr) SetData(_ string, _ interface{}) {

}

// GetData is a mock implementation of ErrDataI.GetData
func (a *asErr) GetData(_ string) interface{} {
	return nil
}

// EncodeErrorData is a mock implementation of ErrDataI.EncodeErrorData
func (a *asErr) EncodeErrorData() []byte {
	return nil
}

// Error is a mock implementation of the error interface
func (a *asErr) Error() string { return "asErr" }

// As is a mock implementation of the As method
func (a *asErr) As(target interface{}) bool {
	if te, ok := target.(**asErr); ok {
		*te = a
		return true
	}

	return false
}

// TestError_As tests it the As method of the Error type
func TestError_As(t *testing.T) {
	// Reflection helper for unexported fields
	setField := func(e *Error, field string, v interface{}) {
		rv := reflect.ValueOf(e).Elem().FieldByName(field)
		require.True(t, rv.IsValid(), "field %q not found", field)
		require.True(t, rv.CanAddr(), "field %q not addressable", field)
		reflect.NewAt(rv.Type(), unsafe.Pointer(rv.UnsafeAddr())).Elem().
			Set(reflect.ValueOf(v))
	}

	//----------------------------------------
	// Test cases
	//----------------------------------------
	tests := []struct {
		name   string
		setup  func() (*Error, interface{})
		expect bool
		verify func(t *testing.T, expectOK bool, tgt interface{}, src *Error)
	}{
		{
			name: "nil receiver",
			setup: func() (*Error, interface{}) {
				var tgt *Error
				return nil, &tgt
			},
			expect: false,
			verify: func(t *testing.T, ok bool, tgt interface{}, _ *Error) {
				require.False(t, ok)
				require.Nil(t, *(tgt.(**Error)))
			},
		},
		{
			name: "fast path (same *Error)",
			setup: func() (*Error, interface{}) {
				src := &Error{}
				var tgt *Error
				return src, &tgt
			},
			expect: true,
			verify: func(t *testing.T, ok bool, tgt interface{}, src *Error) {
				require.True(t, ok)
				require.Same(t, src, *(tgt.(**Error)))
			},
		},
		{
			name: "data implements As",
			setup: func() (*Error, interface{}) {
				src := &Error{}
				setField(src, "data", &asErr{})
				var tgt *asErr
				return src, &tgt
			},
			expect: true,
			verify: func(t *testing.T, ok bool, tgt interface{}, src *Error) {
				require.True(t, ok)
				require.IsType(t, &asErr{}, *(tgt.(**asErr)))
			},
		},
		{
			name: "wrappedErr implements As",
			setup: func() (*Error, interface{}) {
				src := &Error{}
				setField(src, "wrappedErr", &asErr{})
				var tgt *asErr
				return src, &tgt
			},
			expect: true,
			verify: func(t *testing.T, ok bool, tgt interface{}, _ *Error) {
				require.True(t, ok)
				require.IsType(t, &asErr{}, *(tgt.(**asErr)))
			},
		},
		{
			name: "no matching path",
			setup: func() (*Error, interface{}) {
				src := &Error{}
				setField(src, "wrappedErr", errors.New("plain"))
				var tgt *asErr
				return src, &tgt
			},
			expect: false,
			verify: func(t *testing.T, ok bool, tgt interface{}, _ *Error) {
				require.False(t, ok)
				require.Nil(t, *(tgt.(**asErr)))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src, tgt := tc.setup()
			ok := src.As(tgt)
			tc.verify(t, ok, tgt, src)
		})
	}
}

// TestError_SetWrappedErr tests the SetWrappedErr method of the Error type.
func TestError_SetWrappedErr(t *testing.T) {
	t.Run("nil receiver", func(t *testing.T) {
		var e *Error

		require.NotPanics(t, func() {
			e.SetWrappedErr(errors.New("should be ignored"))
		})
	})

	t.Run("set initial wrapped error", func(t *testing.T) {
		e := NewError("root")
		wrapped := NewError("wrapped1")

		e.SetWrappedErr(wrapped)

		var result *Error

		require.True(t, errors.As(e.wrappedErr, &result))
		require.Equal(t, "ERROR (9): wrapped1", result.Error())
	})

	/*
		// Does not work

		t.Run("non-*Error in wrappedErr breaks chain", func(t *testing.T) {
			e := NewError("root")
			e.SetWrappedErr(errors.New("non-wrapped std error"))

			// try appending another one — chain should reset
			wrapped := NewError("new-error")
			e.SetWrappedErr(wrapped)

			var result *Error
			require.True(t, errors.As(e.wrappedErr, &result), "should reset to new error")
			require.Equal(t, "new-error", result.Error())

			// ensure the original stdlib error is gone from a chain
			require.Nil(t, result.wrappedErr)
		})
	*/

	t.Run("append to existing wrapped chain", func(t *testing.T) {
		e := NewError("root")
		wrapped1 := NewError("wrapped1")
		wrapped2 := NewError("wrapped2")

		e.SetWrappedErr(wrapped1)
		e.SetWrappedErr(wrapped2)

		var second *Error

		require.True(t, errors.As(e.wrappedErr, &second))
		require.Equal(t, "ERROR (9): wrapped1 -> ERROR (9): wrapped2", second.Error())

		var third *Error

		require.True(t, errors.As(second.wrappedErr, &third))
		require.Equal(t, "ERROR (9): wrapped2", third.Error())
	})
}

// TestError_Unwrap tests the Unwrap method of the Error type.
func TestError_Unwrap(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var e *Error

		require.Nil(t, e.Unwrap())
	})

	t.Run("no wrappedErr returns nil", func(t *testing.T) {
		e := NewError("no wrap")
		require.Nil(t, e.Unwrap())
	})

	t.Run("returns standard wrapped error", func(t *testing.T) {
		inner := errors.New("inner error")
		e := NewError("outer")
		e.SetWrappedErr(inner)

		require.Same(t, inner, e.Unwrap())
	})

	t.Run("returns *Error wrapped error", func(t *testing.T) {
		inner := NewError("inner custom error")
		e := NewError("outer")
		e.SetWrappedErr(inner)

		unwrapped := e.Unwrap()
		require.Same(t, inner, unwrapped)

		var target *Error

		require.True(t, errors.As(unwrapped, &target))
		require.Equal(t, "ERROR (9): inner custom error", target.Error())
	})

	t.Run("works with errors.Unwrap", func(t *testing.T) {
		inner := errors.New("stdlib inner")
		e := NewError("top level")
		e.SetWrappedErr(inner)

		require.Same(t, inner, errors.Unwrap(e))
	})
}

// TestError_Code tests the Code method of the Error type.
func TestError_Code(t *testing.T) {
	t.Run("nil receiver returns ERR_UNKNOWN", func(t *testing.T) {
		var e *Error

		require.Equal(t, ERR_UNKNOWN, e.Code())
	})

	t.Run("returns correct general error code", func(t *testing.T) {
		e := &Error{code: ERR_INVALID_ARGUMENT}
		require.Equal(t, ERR_INVALID_ARGUMENT, e.Code())
	})

	t.Run("returns correct block error code", func(t *testing.T) {
		e := &Error{code: ERR_BLOCK_NOT_FOUND}
		require.Equal(t, ERR_BLOCK_NOT_FOUND, e.Code())
	})

	t.Run("returns correct tx error code", func(t *testing.T) {
		e := &Error{code: ERR_TX_INVALID}
		require.Equal(t, ERR_TX_INVALID, e.Code())
	})

	t.Run("returns correct service error code", func(t *testing.T) {
		e := &Error{code: ERR_SERVICE_UNAVAILABLE}
		require.Equal(t, ERR_SERVICE_UNAVAILABLE, e.Code())
	})

	t.Run("returns correct utxo error code", func(t *testing.T) {
		e := &Error{code: ERR_UTXO_NOT_FOUND}
		require.Equal(t, ERR_UTXO_NOT_FOUND, e.Code())
	})

	t.Run("returns correct kafka error code", func(t *testing.T) {
		e := &Error{code: ERR_KAFKA_DECODE_ERROR}
		require.Equal(t, ERR_KAFKA_DECODE_ERROR, e.Code())
	})

	t.Run("returns correct blob error code", func(t *testing.T) {
		e := &Error{code: ERR_BLOB_EXISTS}
		require.Equal(t, ERR_BLOB_EXISTS, e.Code())
	})

	t.Run("returns correct state error code", func(t *testing.T) {
		e := &Error{code: ERR_STATE_INITIALIZATION}
		require.Equal(t, ERR_STATE_INITIALIZATION, e.Code())
	})

	t.Run("returns correct network error code", func(t *testing.T) {
		e := &Error{code: ERR_INVALID_IP}
		require.Equal(t, ERR_INVALID_IP, e.Code())
	})
}

// TestError_Message tests the Message method of the Error type.
func TestError_Message(t *testing.T) {
	t.Run("nil receiver returns empty string", func(t *testing.T) {
		var e *Error

		require.Equal(t, "", e.Message())
	})

	t.Run("returns explicitly set message", func(t *testing.T) {
		msg := "something went wrong"
		e := &Error{message: msg}
		require.Equal(t, msg, e.Message())
	})

	t.Run("returns empty string when message is empty", func(t *testing.T) {
		e := &Error{message: ""}
		require.Equal(t, "", e.Message())
	})

	t.Run("returns message with newline characters", func(t *testing.T) {
		msg := "first line\nsecond line"
		e := &Error{message: msg}
		require.Equal(t, msg, e.Message())
	})

	t.Run("returns message with special characters", func(t *testing.T) {
		msg := "error: 💥 something \"weird\" happened @ line #42"
		e := &Error{message: msg}
		require.Equal(t, msg, e.Message())
	})
}

// TestError_WrappedErr tests the WrappedErr method of the Error type.
func TestError_WrappedErr(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var e *Error

		require.Nil(t, e.WrappedErr())
	})

	t.Run("no wrappedErr returns nil", func(t *testing.T) {
		e := &Error{message: "no wrap"}
		require.Nil(t, e.WrappedErr())
	})

	t.Run("wrappedErr is standard error", func(t *testing.T) {
		inner := errors.New("inner std error")
		e := &Error{wrappedErr: inner}
		require.Same(t, inner, e.WrappedErr())
	})

	t.Run("wrappedErr is custom *Error", func(t *testing.T) {
		inner := &Error{message: "inner custom"}
		e := &Error{wrappedErr: inner}
		require.Same(t, inner, e.WrappedErr())

		var target *Error

		require.True(t, errors.As(e.WrappedErr(), &target))
		require.Equal(t, "inner custom", target.message)
	})

	t.Run("wrappedErr returns only first level", func(t *testing.T) {
		deep := &Error{message: "deepest"}
		mid := &Error{wrappedErr: deep}
		top := &Error{wrappedErr: mid}

		require.Same(t, mid, top.WrappedErr())
		require.NotSame(t, deep, top.WrappedErr())
	})
}

// mockErrData is a mock implementation of the ErrDataI interface for testing purposes.
type mockErrData struct {
	entries map[string]interface{}
}

// Error is a mock implementation of the error interface for the mockErrData type.
func (m *mockErrData) Error() string {
	return "mock error"
}

// SetData is a mock implementation of the ErrDataI.SetData method for the mockErrData type.
func (m *mockErrData) SetData(key string, value interface{}) {
	if m.entries == nil {
		m.entries = make(map[string]interface{})
	}

	m.entries[key] = value
}

// GetData is a mock implementation of the ErrDataI.GetData method for the mockErrData type.
func (m *mockErrData) GetData(key string) interface{} {
	return m.entries[key]
}

// EncodeErrorData is a mock implementation of the ErrDataI.EncodeErrorData method for the mockErrData type.
func (m *mockErrData) EncodeErrorData() []byte {
	return []byte("encoded")
}

// TestError_Data tests the Data method of the Error type.
func TestError_Data(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var e *Error

		require.Nil(t, e.Data())
	})

	t.Run("nil data field returns nil", func(t *testing.T) {
		e := &Error{data: nil}
		require.Nil(t, e.Data())
	})

	t.Run("returns custom ErrDataI implementation", func(t *testing.T) {
		mock := &mockErrData{}
		mock.SetData("foo", 42)

		e := &Error{data: mock}
		data := e.Data()

		require.NotNil(t, data)
		require.Implements(t, (*ErrDataI)(nil), data)

		val := data.GetData("foo")
		require.Equal(t, 42, val)

		encoded := data.EncodeErrorData()
		require.Equal(t, []byte("encoded"), encoded)
	})
}

// TestError_GetData tests the GetData method of the Error type.
func TestError_GetData(t *testing.T) {
	t.Run("nil receiver returns nil", func(t *testing.T) {
		var e *Error
		val := e.GetData("missing")
		require.Nil(t, val)
	})

	t.Run("nil data field returns nil", func(t *testing.T) {
		e := &Error{data: nil}
		val := e.GetData("anything")
		require.Nil(t, val)
	})

	t.Run("returns value for existing key", func(t *testing.T) {
		mock := &mockErrData{}
		mock.SetData("key1", "value1")
		mock.SetData("key2", 123)

		e := &Error{data: mock}

		val1 := e.GetData("key1")
		require.Equal(t, "value1", val1)

		val2 := e.GetData("key2")
		require.Equal(t, 123, val2)
	})

	t.Run("returns nil for non-existing key", func(t *testing.T) {
		mock := &mockErrData{}
		mock.SetData("existing", "something")

		e := &Error{data: mock}

		val := e.GetData("missing")
		require.Nil(t, val)
	})
}

// TestTError_Error tests the Error method of the TError type.
func TestTError_Error(t *testing.T) {
	t.Run("nil receiver returns <nil>", func(t *testing.T) {
		var x *TError

		require.Equal(t, "<nil>", x.Error())
	})

	t.Run("IsNil() true returns <nil>", func(t *testing.T) {
		x := &TError{}
		require.Equal(t, "<nil>", x.Error())
	})

	t.Run("no WrappedError formats error correctly", func(t *testing.T) {
		x := &TError{
			Code:    ERR_TX_NOT_FOUND,
			Message: "transaction not found",
		}

		expected := fmt.Sprintf("TX_NOT_FOUND (%d): transaction not found", ERR_TX_NOT_FOUND)
		require.Equal(t, expected, x.Error())
	})

	t.Run("with WrappedError formats full chain", func(t *testing.T) {
		inner := &TError{
			Code:    ERR_BLOCK_NOT_FOUND,
			Message: "block not found",
		}

		x := &TError{
			Code:         ERR_TX_INVALID,
			Message:      "invalid tx",
			WrappedError: inner,
		}

		expected := fmt.Sprintf("TX_INVALID (%d): invalid tx -> %v", ERR_TX_INVALID, inner)
		require.Equal(t, expected, x.Error())
	})

	t.Run("with nested wrapped error stringifies both levels", func(t *testing.T) {
		innerMost := &TError{
			Code:    ERR_CONFIGURATION,
			Message: "bad config",
		}

		inner := &TError{
			Code:         ERR_SERVICE_ERROR,
			Message:      "service error",
			WrappedError: innerMost,
		}

		top := &TError{
			Code:         ERR_STORAGE_ERROR,
			Message:      "storage failure",
			WrappedError: inner,
		}

		expected := fmt.Sprintf("STORAGE_ERROR (%d): storage failure -> %v", ERR_STORAGE_ERROR, inner)
		require.Equal(t, expected, top.Error())
	})
}

// TestErrorCodeToGRPCCode tests the ErrorCodeToGRPCCode function to ensure it maps error codes to gRPC codes correctly.
func TestErrorCodeToGRPCCode(t *testing.T) {
	tests := []struct {
		name     string
		errCode  ERR
		expected codes.Code
	}{
		{
			name:     "maps ERR_UNKNOWN to codes.Unknown",
			errCode:  ERR_UNKNOWN,
			expected: codes.Unknown,
		},
		{
			name:     "maps ERR_INVALID_ARGUMENT to codes.InvalidArgument",
			errCode:  ERR_INVALID_ARGUMENT,
			expected: codes.InvalidArgument,
		},
		{
			name:     "maps ERR_THRESHOLD_EXCEEDED to codes.ResourceExhausted",
			errCode:  ERR_THRESHOLD_EXCEEDED,
			expected: codes.ResourceExhausted,
		},
		{
			name:     "unmapped code TX_INVALID defaults to codes.Internal",
			errCode:  ERR_TX_INVALID,
			expected: codes.Internal,
		},
		{
			name:     "unmapped code BLOCK_NOT_FOUND defaults to codes.Internal",
			errCode:  ERR_BLOCK_NOT_FOUND,
			expected: codes.Internal,
		},
		{
			name:     "unmapped code STORAGE_ERROR defaults to codes.Internal",
			errCode:  ERR_STORAGE_ERROR,
			expected: codes.Internal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := ErrorCodeToGRPCCode(tc.errCode)
			require.Equal(t, tc.expected, actual)
		})
	}
}

// TestJoin tests the Join function to ensure it correctly combines multiple errors into a single error message.
func TestJoin(t *testing.T) {
	t.Run("no errors passed returns nil", func(t *testing.T) {
		err := Join()
		require.Nil(t, err)
	})

	t.Run("all nil errors returns nil", func(t *testing.T) {
		err := Join(nil, nil)
		require.Nil(t, err)
	})

	t.Run("single non-nil error returns that error", func(t *testing.T) {
		err := Join(errors.New("something failed"))
		require.NotNil(t, err)
		require.Equal(t, "something failed", err.Error())
	})

	t.Run("multiple errors are joined", func(t *testing.T) {
		err1 := errors.New("first issue")
		err2 := errors.New("second issue")
		err := Join(err1, err2)
		require.NotNil(t, err)
		require.Equal(t, "first issue, second issue", err.Error())
	})

	t.Run("mixed nil and non-nil errors", func(t *testing.T) {
		err1 := errors.New("only one real issue")
		err := Join(nil, err1, nil)
		require.NotNil(t, err)
		require.Equal(t, "only one real issue", err.Error())
	})

	t.Run("errors with punctuation and newlines", func(t *testing.T) {
		err1 := errors.New("line one\n")
		err2 := errors.New("tab\tseparated")
		err3 := errors.New("unicode 💥")

		err := Join(err1, err2, err3)
		expected := "line one\n, tab\tseparated, unicode 💥"
		require.Equal(t, expected, err.Error())
	})
}

// TestError_Format tests the formatting of the Error type using fmt.Sprintf.
func TestError_Format(t *testing.T) {
	base := &Error{
		code:     ERR_TX_INVALID,
		message:  "invalid transaction",
		file:     "tx_handler.go",
		line:     42,
		function: "ValidateTransaction",
	}

	t.Run("format with %%s returns Error()", func(t *testing.T) {
		out := fmt.Sprintf("%s", base)
		require.Equal(t, base.Error(), out)
	})

	t.Run("format with %%v returns Error()", func(t *testing.T) {
		out := fmt.Sprintf("%v", base)
		require.Equal(t, base.Error(), out)
	})

	t.Run("format with %%+v includes stack trace", func(t *testing.T) {
		out := fmt.Sprintf("%+v", base)

		require.Contains(t, out, base.Error())
		require.Contains(t, out, "ValidateTransaction")
		require.Contains(t, out, "tx_handler.go:42")
		require.Contains(t, out, strconv.Itoa(int(base.code)))
		require.Contains(t, out, base.message)
	})

	t.Run("format with %%#v includes stack trace", func(t *testing.T) {
		out := fmt.Sprintf("%#v", base)

		require.Contains(t, out, base.Error())
		require.Contains(t, out, "ValidateTransaction")
		require.Contains(t, out, "tx_handler.go:42")
	})

	t.Run("nested wrapped error includes nested stack trace", func(t *testing.T) {
		inner := &Error{
			code:     ERR_BLOCK_NOT_FOUND,
			message:  "block not found",
			file:     "block_lookup.go",
			line:     21,
			function: "FindBlock",
		}

		base.wrappedErr = inner

		out := fmt.Sprintf("%+v", base)

		require.Contains(t, out, base.Error())
		require.Contains(t, out, "ValidateTransaction")
		require.Contains(t, out, "tx_handler.go:42")

		// Inner stack trace
		require.Contains(t, out, "FindBlock")
		require.Contains(t, out, "block_lookup.go:21")
		require.Contains(t, out, inner.message)
	})
}

// TestError_buildStackTrace tests the buildStackTrace method of the Error type to ensure it formats the stack trace correctly.
func TestError_buildStackTrace(t *testing.T) {
	t.Run("single error with basic fields", func(t *testing.T) {
		e := &Error{
			code:     ERR_TX_INVALID,
			message:  "invalid tx",
			file:     "tx.go",
			line:     123,
			function: "Validate",
		}

		trace := e.buildStackTrace()
		require.Contains(t, trace, "Validate() tx.go:123")
		require.Contains(t, trace, "[31] invalid tx") // 31 = TX_INVALID
		require.Contains(t, trace, "\n- ")
	})

	t.Run("wrapped error appends nested stack trace", func(t *testing.T) {
		inner := &Error{
			code:     ERR_BLOCK_NOT_FOUND,
			message:  "missing block",
			file:     "block.go",
			line:     88,
			function: "FindBlock",
		}

		outer := &Error{
			code:       ERR_TX_INVALID,
			message:    "tx error",
			file:       "tx.go",
			line:       123,
			function:   "Validate",
			wrappedErr: inner,
		}

		trace := outer.buildStackTrace()

		// Outer
		require.Contains(t, trace, "Validate() tx.go:123 [31] tx error")
		// Inner
		require.Contains(t, trace, "FindBlock() block.go:88 [10] missing block")
	})

	t.Run("wrapped error with ERR_UNKNOWN does not recurse", func(t *testing.T) {
		inner := &Error{
			code:     ERR_UNKNOWN,
			message:  "unknown",
			file:     "unknown.go",
			line:     0,
			function: "Mystery",
		}

		outer := &Error{
			code:       ERR_TX_POLICY,
			message:    "policy failure",
			file:       "policy.go",
			line:       77,
			function:   "CheckPolicy",
			wrappedErr: inner,
		}

		trace := outer.buildStackTrace()

		require.Contains(t, trace, "CheckPolicy() policy.go:77 [39] policy failure")
		require.NotContains(t, trace, "Mystery()")
	})

	t.Run("non-*Error wrappedErr is ignored", func(t *testing.T) {
		outer := &Error{
			code:       ERR_TX_ERROR,
			message:    "tx general failure",
			file:       "tx.go",
			line:       999,
			function:   "HandleTx",
			wrappedErr: errors.New("something went wrong"),
		}

		trace := outer.buildStackTrace()
		require.Contains(t, trace, "HandleTx() tx.go:999 [49] tx general failure")
		require.NotContains(t, trace, "something went wrong")
	})
}

// TestNew_InvalidCodeTriggersFallback tests that New function handles invalid error codes correctly.
func TestNew_InvalidCodeTriggersFallback(t *testing.T) {
	t.Run("returns error with fallback message when code is undefined", func(t *testing.T) {
		invalidCode := ERR(9999)

		e := New(invalidCode, "this should not be used")

		require.NotNil(t, e)
		require.Equal(t, invalidCode, e.Code())
		require.Equal(t, "invalid error code", e.Message())
		require.True(t, e.line > 0)
	})

	t.Run("preserves wrapped error when code is invalid", func(t *testing.T) {
		wrapped := errors.New("deep issue")
		invalidCode := ERR(9999)

		e := New(invalidCode, "should be replaced", wrapped)

		require.NotNil(t, e)
		require.Equal(t, invalidCode, e.Code())
		require.Equal(t, "invalid error code", e.Message())
		require.NotNil(t, e.WrappedErr())
		require.Contains(t, e.WrappedErr().Error(), "deep issue")
	})
}
