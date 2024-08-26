package errors

import (
	"errors"
	"fmt"
	reflect "reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type ErrData interface {
	Error() string
}

type Error struct {
	Code       ERR
	Message    string
	WrappedErr error
	Data       ErrData
}

func (e *Error) Error() string {
	// Error() can be called on wrapped errors, which can be nil, for example predefined errors
	if e == nil {
		return "<nil>"
	}

	dataMsg := ""
	if e.Data != nil {
		dataMsg = e.Data.Error() // Call Error() on the ErrorData
	}

	if e.WrappedErr == nil {
		if dataMsg == "" {
			return fmt.Sprintf("%d: %v", e.Code, e.Message)
		}
		return fmt.Sprintf("%d: %v, data: %s", e.Code, e.Message, dataMsg)
	}

	if dataMsg == "" {
		return fmt.Sprintf("Error: %s (error code: %d), %v: %v", e.Code.Enum(), e.Code, e.Message, e.WrappedErr)
	}

	return fmt.Sprintf("Error: %s (error code: %d), %v: %v, data: %s", e.Code.Enum(), e.Code, e.Message, e.WrappedErr, dataMsg)
}

// Is reports whether error codes match.
func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}

	var ue *Error
	if errors.As(target, &ue) {
		if e.Code == ue.Code {
			return true
		}

		if e.WrappedErr == nil {
			return false
		}
	}

	// process, storage, block not found, tx_missing
	// errors.Is(storage, err) true

	// Unwrap the current error and recursively call Is on the unwrapped error
	if unwrapped := errors.Unwrap(e); unwrapped != nil {
		if ue, ok := unwrapped.(*Error); ok {
			return ue.Is(target)
		}
	}

	return false
}

func (e *Error) As(target interface{}) bool {
	// fmt.Println("In as, e:", e, "\ntarget: ", target)
	if e == nil {
		return false
	}

	// Try to assign this error to the target if the types are compatible
	if targetErr, ok := target.(**Error); ok {
		*targetErr = e
		return true
	}

	// check if Data matches the target type
	if e.Data != nil {
		if data, ok := e.Data.(error); ok {
			return errors.As(data, target)
		}
	}

	// Recursively check the wrapped error if there is one
	if e.WrappedErr != nil {
		// use reflect to see if the value is nil. If it is, return false
		if reflect.ValueOf(e.WrappedErr).IsNil() {
			return false
		}
		return errors.As(e.WrappedErr, target)
	}

	// Also check any further unwrapped errors
	if unwrapped := errors.Unwrap(e); unwrapped != nil {
		return errors.As(unwrapped, target)
	}

	return false
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.WrappedErr
}

func New(code ERR, message string, params ...interface{}) *Error {
	var wErr *Error

	// Extract the wrapped error, if present
	if len(params) > 0 {
		lastParam := params[len(params)-1]

		if err, ok := lastParam.(*Error); ok {
			wErr = err
			params = params[:len(params)-1]
		} else if err, ok := lastParam.(error); ok {
			wErr = &Error{Message: err.Error()}
			params = params[:len(params)-1]
		}
	}

	// Format the message with the remaining parameters
	if len(params) > 0 {
		err := fmt.Errorf(message, params...)
		message = err.Error()
	}

	// Check if the code exists in the ErrorConstants enum
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

func WrapGRPC(err *Error) *Error {
	if err == nil {
		return nil
	}

	// If the error is already a *Error, wrap it with gRPC details
	details, _ := anypb.New(&TError{
		Code:         err.Code,
		Message:      err.Message,
		WrappedError: err.WrappedErr.Error(),
	})

	st := status.New(ErrorCodeToGRPCCode(err.Code), err.Message)
	st, detailsErr := st.WithDetails(details)
	if detailsErr != nil {
		// the following should not be used.
		return &Error{
			// TODO: add a specific error code for this case, and check it when it is used
			Code:       ERR_ERROR,
			Message:    "error adding details to the error's gRPC status",
			WrappedErr: err,
		}
	}

	// if GPRC error is successfully created
	return &Error{
		Code:       err.Code,
		Message:    err.Message,
		WrappedErr: st.Err(),
	}
}

func UnwrapGRPC(err *Error) *Error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err.WrappedErr)
	if !ok {
		return err // Not a gRPC status error
	}

	// Attempt to extract and return detailed UBSVError if present
	for _, detail := range st.Details() {
		var ubsvErr TError
		if err := anypb.UnmarshalTo(detail.(*anypb.Any), &ubsvErr, proto.UnmarshalOptions{}); err == nil {
			if ubsvErr.WrappedError != "" {
				return New(ubsvErr.Code, ubsvErr.Message, fmt.Errorf(ubsvErr.WrappedError))
			} else {
				return New(ubsvErr.Code, ubsvErr.Message)
			}
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
	return errors.As(err, target)
}
