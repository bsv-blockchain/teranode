package errors

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors/grpctest/github.com/bitcoin-sv/ubsv/errors/grpctest"
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

func TestErrorIs(t *testing.T) {
	err := New(ERR_NOT_FOUND, "not found")
	require.True(t, err.Is(ErrNotFound))

	err = New(ERR_BLOCK_INVALID, "invalid block error")
	require.True(t, err.Is(ErrBlockInvalid))
}

func ReturnsError() error {
	return NewTxNotFoundError("Tx not found")
}

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

func Test_UtxoSpentError(t *testing.T) {
	t.Run("UtxoSpentError", func(t *testing.T) {
		txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}

		utxoSpentError := NewUtxoSpentError(txID, 1, chainhash.Hash{}, chainhash.Hash{})
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
		assert.Equal(t, chainhash.Hash{}, spentErr.data.(*UtxoSpentErrData).SpendingTxHash)

		spendingTxHash := spentErr.GetData("spending_tx_hash")
		assert.Equal(t, chainhash.Hash{}, spendingTxHash)
	})

	t.Run("UtxoSpentErrorData grpc", func(t *testing.T) {
		txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}
		utxoSpentError := NewUtxoSpentError(txID, 1, chainhash.Hash{}, chainhash.Hash{})

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
		assert.Equal(t, chainhash.Hash{}, unwrappedErr.Data().(*UtxoSpentErrData).SpendingTxHash)

		spendingTxHash := unwrappedErr.Data().GetData("spending_tx_hash")
		assert.Equal(t, chainhash.Hash{}, spendingTxHash)

		contains := "60: UTXO_SPENT (60): 0000000000000000000000000000000000000000000000313233343536373839:1 utxo already spent by tx id 0000000000000000000000000000000000000000000000000000000000000000 \"utxo 0000000000000000000000000000000000000000000000313233343536373839 already spent by 0000000000000000000000000000000000000000000000000000000000000000\""

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

func TestJoinWithMultipleErrs(t *testing.T) {
	err1 := New(ERR_NOT_FOUND, "not found")
	err2 := New(ERR_BLOCK_NOT_FOUND, "block not found")
	err3 := New(ERR_INVALID_ARGUMENT, "invalid argument")

	joinedErr := Join(err1, err2, err3)
	require.NotNil(t, joinedErr)
	assert.Equal(t, "NOT_FOUND (3): not found, BLOCK_NOT_FOUND (10): block not found, INVALID_ARGUMENT (1): invalid argument", joinedErr.Error())
}

func TestErrorString(t *testing.T) {
	err := errors.New("some error")

	thisErr := NewStorageError("failed to set data from reader [%s:%s]", "bucket", "key", err)

	assert.Equal(t, "STORAGE_ERROR (59): failed to set data from reader [bucket:key] -> UNKNOWN (0): some error", thisErr.Error())
}

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

func ReturnErrorAsStandardErrorWithoutModification(error *Error) error {
	return error
}

func ReturnSpecialErrorFromStandardErrorWithModification(error error) *Error {
	return NewError("error on the top", error)
}

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
	// fmt.Println("Scenario 2 error:\n", processingError)
}

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

	defer clientConn.Close()

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
	standardErrResourceExhausted := fmt.Errorf("Resource exhausted")
	grpcErr = status.Error(codes.ResourceExhausted, standardErrResourceExhausted.Error())
	wrappedErrResourceExhausted := WrapGRPC(grpcErr)

	// Unwrap the gRPC error using the UnwrapGRPC function
	unwrappedResourceExhausted := UnwrapGRPC(wrappedErrResourceExhausted)

	// Ensure that the unwrapped error is not nil
	require.NotNil(t, unwrappedResourceExhausted)

	// Check that the unwrapped error contains the correct message and code
	require.Equal(t, ERR_ERROR, unwrappedResourceExhausted.Code())
	require.Equal(t, "rpc error: code = ResourceExhausted desc = Resource exhausted", unwrappedResourceExhausted.Message())
}

type server struct {
	grpctest.UnimplementedTestServiceServer
}

func (s *server) TestMethod(ctx context.Context, req *grpctest.TestRequest) (*grpctest.TestResponse, error) {
	// Simulate an error
	txID := chainhash.Hash{'9', '8', '7', '6', '5', '4', '3', '2', '1'}
	utxoHash := chainhash.Hash{'1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
	spendingTxID := chainhash.Hash{'1', '2', '3', '4', '5', '6', '7', '8', '9'}

	baseErr := NewUtxoSpentError(txID, 10, utxoHash, spendingTxID)
	level1Err := NewTxInvalidError("transaction invalid", baseErr)
	level2Err := NewBlockInvalidError("block invalid", level1Err)
	level3Err := NewServiceError("level service error", level2Err)
	level4Err := NewContextCanceledError("top level context error", level3Err)

	return nil, WrapGRPC(level4Err)
}

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

	defer clientConn.Close()

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

func TestIsErrorWithNestedErrorCodesWithWrapGRPC(t *testing.T) {
	errRoot := NewServiceError("service error")
	err := NewProcessingError("processing error", errRoot)
	grpcErr := WrapGRPC(err)

	require.True(t, errRoot.Is(ErrServiceError))
	require.True(t, err.Is(ErrProcessing))
	require.True(t, Is(grpcErr, ErrServiceError))
	require.True(t, Is(grpcErr, ErrProcessing))
}
