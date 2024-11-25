package errors

import (
	"errors"
	"fmt"
	reflect "reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/anypb"
)

type ErrData interface {
	Error() string
}

type Error struct {
	code       ERR
	message    string
	wrappedErr error
	data       ErrData
}

type Interface interface {
	Error() string
	Is(target error) bool
	As(target interface{}) bool
	Unwrap() error

	Code() ERR
	Message() string
	WrappedErr() error
	Data() ErrData
}

func (e *Error) Error() string {
	// Error() can be called on wrapped errors, which can be nil, for example predefined errors
	if e == nil {
		return "<nil>"
	}

	dataMsg := ""
	if e.Data() != nil {
		dataMsg = e.data.Error() // Call Error() on the ErrorData
	}

	if e.WrappedErr() == nil {
		if dataMsg == "" {
			return fmt.Sprintf("Error: %s, (error code: %d), Message: %v", e.code.Enum(), e.code, e.message)
		}
		return fmt.Sprintf("%d: %v, data: %s", e.code, e.message, dataMsg)
	}

	if dataMsg == "" {
		return fmt.Sprintf("Error: %s (error code: %d), Message: %v, Wrapped err: %v", e.code.Enum(), e.code, e.message, e.wrappedErr)
	}

	return fmt.Sprintf("Error: %s (error code: %d), Message: %v, Wrapped err: %v, Data: %s", e.code.Enum(), e.code, e.message, e.wrappedErr, dataMsg)
}

// Is reports whether error codes match.
func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}

	targetError, ok := target.(*Error)
	if !ok {
		// fmt.Println("Target is not of type *Error, checking \ne.Error()", e.Error(), "contains() target.Error():\n", target.Error())
		return strings.Contains(e.Error(), target.Error())
	}

	if e.code == targetError.code {
		return true
	}

	if e.wrappedErr == nil {
		return false
	}

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
	if e.data != nil {
		if data, ok := e.data.(error); ok {
			return errors.As(data, target)
		}
	}

	// Recursively check the wrapped error if there is one
	if e.wrappedErr != nil {
		// use reflect to see if the value is nil. If it is, return false
		if reflect.ValueOf(e.wrappedErr).IsNil() {
			return false
		}
		return errors.As(e.wrappedErr, target)
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
	return e.wrappedErr
}

func (e *Error) Code() ERR {
	if e == nil {
		return ERR_UNKNOWN
	}
	return e.code
}

func (e *Error) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *Error) WrappedErr() error {
	if e == nil {
		return nil
	}
	return e.wrappedErr
}

func (e *Error) Data() ErrData {
	if e == nil {
		return nil
	}
	return e.data
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
			wErr = &Error{message: err.Error()}
			params = params[:len(params)-1]
		}
	}

	// Format the message with the remaining parameters
	if len(params) > 0 {
		//nolint:forbidigo
		err := fmt.Errorf(message, params...)
		message = err.Error()
	}

	// Check if the code exists in the ErrorConstants enum
	if _, ok := ERR_name[int32(code)]; !ok {
		returnErr := &Error{
			code:    code,
			message: "invalid error code",
		}
		if wErr != nil {
			returnErr.wrappedErr = wErr
		}

		return returnErr
	}

	returnErr := &Error{
		code:    code,
		message: message,
		//WrappedErr: wErr,
	}
	if wErr != nil {
		returnErr.wrappedErr = wErr
	}

	return returnErr
}

// NOTE: GRPC generated code expects this to return error or nil - *Error seems to not evaluate to nil?
// returning *Error seemed to break asset service endpoints that try to delegate to blockchain service
// which in turn breaks the dashboard homepage
func WrapGRPC(err error) error {
	if err == nil {
		return nil
	}

	// If the error is an "*Error", get all wrapped errors, and wrap with gRPC details
	if castedErr, ok := err.(*Error); ok {
		// check if the error is already wrapped, don't wrap it with gRPC details
		if castedErr.wrappedErr != nil {
			if _, ok := status.FromError(castedErr.wrappedErr); ok {
				return err // Already wrapped, skip further wrapping
			}
		}

		var wrappedErrDetails []protoadapt.MessageV1
		// If the error is already an *Error, wrap it with gRPC details
		details, pbError := anypb.New(&TError{
			Code:    castedErr.code,
			Message: castedErr.message,
		})
		if pbError != nil {
			err2 := &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error serializing TError to protobuf Any",
				wrappedErr: err,
			}

			return err2
		}

		wrappedErrDetails = append(wrappedErrDetails, details)
		if castedErr.wrappedErr != nil {
			currWrappedErr := castedErr.wrappedErr
			for currWrappedErr != nil {
				if err, ok := currWrappedErr.(*Error); ok {

					details, _ := anypb.New(&TError{
						Code:    err.code,
						Message: err.message,
					})

					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = err.wrappedErr
				} else {
					details, _ := anypb.New(&TError{
						Code:    ERR_ERROR,
						Message: err.Error(),
					})
					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = nil
				}
			}
		}
		// for i := 0; i < len(wrappedErrDetails); i++ {
		//	fmt.Println("Details for the error is: ", wrappedErrDetails[i])
		// }

		st := status.New(ErrorCodeToGRPCCode(castedErr.code), castedErr.message)
		st, detailsErr := st.WithDetails(wrappedErrDetails...)

		if detailsErr != nil {
			err2 := &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error adding details to the error's gRPC status",
				wrappedErr: err,
			}

			return err2
		}

		return st.Err()
	}

	st := status.New(ErrorCodeToGRPCCode(ErrUnknown.code), ErrUnknown.message)
	details, _ := anypb.New(&TError{
		Code:    ERR_ERROR,
		Message: err.Error(),
	})
	st, detailsErr := st.WithDetails(details)

	if detailsErr != nil {
		// the following should not be used.
		return &Error{
			// TODO: add grpc construction error type
			code:       ERR_ERROR,
			message:    "error adding details to the error's gRPC status",
			wrappedErr: err,
		}
	}

	return &Error{
		code:       ERR_ERROR,
		message:    err.Error(),
		wrappedErr: st.Err(),
	}
}

func UnwrapGRPC(err error) Interface {
	if err == nil {
		return nil
	}

	// If the error is not an "*Error", but "error", unwrap details
	st, ok := status.FromError(err)
	if !ok {
		// return err // Not a gRPC status error
		return &Error{
			code:       ERR_ERROR,
			message:    "error unwrapping gRPC details",
			wrappedErr: err,
		}
	}

	if len(st.Details()) == 0 {
		return &Error{
			code:    ERR_ERROR,
			message: err.Error(),
		}
	}

	var prevErr, currErr *Error

	for i := len(st.Details()) - 1; i >= 0; i-- {
		// Cast the protoadapt.MessageV1 to *anypb.Any, which is what we need to unmarshal
		detail := st.Details()[i]
		detailAny, ok := detail.(*anypb.Any)

		if !ok {
			// TODO: This should not happen, detail is not of type anypb.Any. What to do here?
			continue // If the detail isn't of the expected type, skip it
		}

		var customDetails TError
		if err := anypb.UnmarshalTo(detailAny, &customDetails, proto.UnmarshalOptions{}); err == nil {
			currErr = New(customDetails.Code, customDetails.Message)

			// if we moved up higher in the hierarchy
			if prevErr != nil {
				currErr.wrappedErr = prevErr
			}

			prevErr = currErr
		}
	}

	return currErr
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

	return errors.New(strings.Join(messages, ", "))
}

func Is(err, target error) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	return errors.Is(err, target)
}

func As(err error, target any) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	return errors.As(err, target)
}

func isGRPCWrappedError(err error) bool {
	_, ok := status.FromError(err)
	return ok
}
