package ubsverrors

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
	ErrNotFound             = New(ERR_NOT_FOUND, "not found")
	ErrBlockNotFound        = New(ERR_BLOCK_NOT_FOUND, "block not found")
	ErrThresholdExceeded    = New(ERR_THRESHOLD_EXCEEDED, "threshold exceeded")
	ErrInvalidBlock         = New(ERR_INVALID_BLOCK, "invalid block")
	ErrInvalidTxDoubleSpend = New(ERR_INVALID_TX_DOUBLE_SPEND, "invalid tx, double spend")
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

	return false
}

func (e *Error) Unwrap() error {
	return e.WrappedErr
}

func New(code ERR, message string, wrappedError ...error) *Error {
	var wErr error
	if len(wrappedError) > 0 {
		wErr = wrappedError[0]
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
		details, _ := anypb.New(&UBSVError{
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
		var ubsvErr UBSVError
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
	case ERR_NOT_FOUND:
		return codes.NotFound
	case ERR_BLOCK_NOT_FOUND:
		return codes.NotFound
	case ERR_THRESHOLD_EXCEEDED:
		return codes.ResourceExhausted
	case ERR_INVALID_BLOCK:
		return codes.Internal
	case ERR_INVALID_TX_DOUBLE_SPEND:
		return codes.Internal
	default:
		return codes.Internal
	}
}
