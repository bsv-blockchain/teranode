package errors

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Error struct {
	Code       ERR
	Message    string
	WrappedErr error
}

var (
	ErrUnknown              = New(ERR_UNKNOWN, "unknown error")
	ErrInvalidArgument      = New(ERR_INVALID_ARGUMENT, "invalid argument")
	ErrThresholdExceeded    = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrNotFound             = New(ERR_NOT_FOUND, "not found")
	ErrProcessing           = New(ERR_PROCESSING, "error processing")
	ErrError                = New(ERR_ERROR, "generic error")
	ErrBlockNotFound        = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrBlockInvalid         = New(ERR_BLOCK_INVALID, "block invalid")
	ErrBlockError           = New(ERR_BLOCK_ERROR, "block error")
	ErrSubtreeNotFound      = New(ERR_SUBTREE_NOT_FOUND, "subtree not found")
	ErrSubtreeInvalid       = New(ERR_SUBTREE_INVALID, "subtree invalid")
	ErrSubtreeError         = New(ERR_SUBTREE_ERROR, "subtree error")
	ErrTxNotFound           = New(ERR_TX_NOT_FOUND, "tx not found")
	ErrTxInvalid            = New(ERR_TX_INVALID, "tx invalid")
	ErrTxInvalidDoubleSpend = New(ERR_TX_INVALID_DOUBLE_SPEND, "tx invalid double spend")
	ErrTxAlreadyExists      = New(ERR_TX_ALREADY_EXISTS, "tx already exists")
	ErrTxError              = New(ERR_TX_ERROR, "tx error")
	ErrServiceUnavailable   = New(ERR_SERVICE_UNAVAILABLE, "service unavailable")
	ErrServiceNotStarted    = New(ERR_SERVICE_NOT_STARTED, "service not started")
	ErrServiceError         = New(ERR_SERVICE_ERROR, "service error")
	ErrStorageUnavailable   = New(ERR_STORAGE_UNAVAILABLE, "storage unavailable")
	ErrStorageNotStarted    = New(ERR_STORAGE_NOT_STARTED, "storage not started")
	ErrStorageError         = New(ERR_STORAGE_ERROR, "storage error")
)

func (e *Error) Error() string {
	if e.WrappedErr == nil {
		return fmt.Sprintf("%d: %v", e.Code, e.Message)
	}

	return fmt.Sprintf("Error: %s (error code: %d),  %v: %v", e.Code.Enum(), e.Code, e.Message, e.WrappedErr)
}

// errors.WrapGRPC(errros.New() )
// fmt.Errorf -> errors.New() will be replaced.

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

func UnwrapGRPC(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err // Not a gRPC status error
	}

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
	// case ERR_NOT_FOUND:
	// 	return codes.NotFound
	// case ERR_BLOCK_NOT_FOUND:
	// 	return codes.NotFound
	case ERR_THRESHOLD_EXCEEDED:
		return codes.ResourceExhausted
	// case ERR_INVALID_BLOCK:
	// 	return codes.Internal
	// case ERR_INVALID_TX_DOUBLE_SPEND:
	// 	return codes.Internal
	// case ERR_SERVICE_UNAVAILABLE:
	// 	return codes.Unavailable
	default:
		return codes.Internal
	}
}

func Join(errs ...error) error {
	return errors.Join(errs...)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func As(err error, target any) bool {
	return errors.As(err, target)
}
