package errors

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Error struct {
	Code       ERR
	Message    string
	WrappedErr error
	Data       map[string]interface{}
}

var (
	ErrUnknown                      = New(ERR_UNKNOWN, "unknown error")
	ErrInvalidArgument              = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrThresholdExceeded            = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrNotFound                     = New(ERR_NOT_FOUND, "not found")
	ErrProcessing                   = New(ERR_PROCESSING, "error processing")
	ErrError                        = New(ERR_ERROR, "generic error")
	ErrBlockNotFound                = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockInvalid                 = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockError                   = New(ERR_BLOCK_ERROR, "block error")
	ErrSubtreeNotFound              = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrSubtreeInvalid               = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeError                 = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrTxNotFound                   = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxInvalid                    = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend         = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxAlreadyExists              = New(ERR_TX_ALREADY_EXISTS, "tx already exists")
	ErrTxCoinbaseMissingBlockHeight = New(ERR_TX_COINBASE_MISSING_BLOCK_HEIGHT, "the coinbase signature script doesn't have the block height")
	ErrTxError                      = New(ERR_TX_ERROR, "tx error")
	ErrUtxoNotFound                 = New(ERR_UTXO_NOT_FOUND, "utxo not found")
	ErrUtxoAlreadyExists            = New(ERR_UTXO_ALREADY_EXISTS, "utxo already exists")
	ErrUtxoSpent                    = New(ERR_UTXO_SPENT, "utxo spent")
	ErrUtxoLocked                   = New(ERR_UTXO_LOCK_TIME, "utxo locked")
	ErrUtxoError                    = New(ERR_UTXO_ERROR, "utxo error")
	ErrServiceUnavailable           = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrServiceNotStarted            = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceError                 = New(ERR_SERVICE_ERROR, "service error")
	ErrStorageUnavailable           = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrStorageNotStarted            = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageError                 = New(ERR_STORAGE_ERROR, "storage error")
)

func (e *Error) Error() string {
	if e.WrappedErr == nil {
		return fmt.Sprintf("%d: %v", e.Code, e.Message)
	}

	return fmt.Sprintf("Error: %s (error code: %d),  %v: %v", e.Code.Enum(), e.Code, e.Message, e.WrappedErr)
}

// Is reports whether error codes match.
func (e *Error) Is(target error) bool {
	var ue *Error
	if errors.As(target, &ue) {
		return e.Code == ue.Code
	}

	return errors.Is(errors.Unwrap(e), target)
}

func (e *Error) Unwrap() error {
	return e.WrappedErr
}

func (e *Error) WithData(key string, value interface{}) *Error {
	if e.Data == nil {
		e.Data = make(map[string]interface{})
	}
	e.Data[key] = value

	return e
}

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

		// TODO add the Data field to the details

		return st.Err()
	}

	return status.New(ErrorCodeToGRPCCode(ErrUnknown.Code), ErrUnknown.Message).Err()
}

func UnwrapGRPC(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err // Not a gRPC status error
	}

	// TODO add the Data field to the details

	// Attempt to extract and return detailed UBSVError if present
	for _, detail := range st.Details() {
		var ubsvErr TError
		if err := anypb.UnmarshalTo(detail.(*anypb.Any), &ubsvErr, proto.UnmarshalOptions{}); err == nil {
			return New(ubsvErr.Code, ubsvErr.Message)
		}
	}

	// Fallback: Map common gRPC status codes to custom error codes
	switch st.Code() {
	case codes.NotFound:
		return New(ERR_NOT_FOUND, st.Message())
	case codes.InvalidArgument:
		return New(ERR_INVALID_ARGUMENT, st.Message())
	case codes.ResourceExhausted:
		return New(ERR_THRESHOLD_EXCEEDED, st.Message())
	case codes.Unknown:
		return New(ErrUnknown.Code, st.Message())
	default:
		// For unhandled cases, return ErrUnknown with the original gRPC error message
		return New(ErrUnknown.Code, st.Message())
	}
}

// ErrorCodeToGRPCCode maps your application-specific error codes to gRPC status codes.
func ErrorCodeToGRPCCode(code ERR) codes.Code {
	switch code {
	case ERR_UNKNOWN:
		return codes.Unknown
	case ERR_INVALID_ARGUMENT:
		return codes.InvalidArgument
	case ERR_THRESHOLD_EXCEEDED:
		return codes.ResourceExhausted
	default:
		return codes.Internal
	}
}

func Join(errs ...error) error {
	var messages []string
	for _, err := range errs {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf(strings.Join(messages, ", "))
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	// TODO implement this for our errors
	return errors.As(err, target)
}
