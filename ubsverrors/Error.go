package ubsverrors

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Error struct {
	Code       ErrorConstants
	Message    string
	WrappedErr error
}

var (
	ErrUnknown           = New(ErrorConstants_UNKNOWN, "unknown error")
	ErrInvalidArgument   = New(ErrorConstants_INVALID_ARGUMENT, "invalid argument")
	ErrNotFound          = New(ErrorConstants_NOT_FOUND, "not found")
	ErrBlockNotFound     = New(ErrorConstants_BLOCK_NOT_FOUND, "block not found")
	ErrThresholdExceeded = New(ErrorConstants_THRESHOLD_EXCEEDED, "threshold exceeded")
)

func (e *Error) Error() string {
	if e.WrappedErr == nil {
		return fmt.Sprintf("%d: %v", e.Code, e.Message)
	}

	return fmt.Sprintf("%d: %v: %v", e.Code, e.Message, e.WrappedErr)
}

func (e *Error) Is(target error) bool {
	if ue, ok := target.(*Error); ok {
		return e.Code == ue.Code
	}

	return false
}

func (e *Error) Unwrap() error {
	return e.WrappedErr
}

func New(code ErrorConstants, message string, wrappedError ...error) error {
	// Check the code exists in the ErrorConstants enum
	if _, ok := ErrorConstants_name[int32(code)]; !ok {
		return fmt.Errorf("invalid error code: %d for error %q", code, message)
	}

	var wErr error
	if len(wrappedError) > 0 {
		wErr = wrappedError[0]
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
		st := status.New(codes.Internal, err.Error())

		details := &UBSVError{
			Code:    uErr.Code,
			Message: uErr.Message,
		}

		st, _ = st.WithDetails(details)

		return st.Err()
	}

	return err
}

func UnwrapGRPC(err error) error {
	if err == nil {
		return nil
	}

	if st, ok := status.FromError(err); ok {
		for _, detail := range st.Details() {
			if t, ok := detail.(*UBSVError); ok {
				return &Error{
					Code:    t.Code,
					Message: t.Message,
				}
			}
		}
	}

	return err
}
