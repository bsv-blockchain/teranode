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
			return fmt.Sprintf("Error: %s, (error code: %d), Message: %v", e.Code.Enum(), e.Code, e.Message)
		}
		return fmt.Sprintf("%d: %v, data: %s", e.Code, e.Message, dataMsg)
	}

	if dataMsg == "" {
		return fmt.Sprintf("Error: %s (error code: %d), Message: %v, Wrapped err: %v", e.Code.Enum(), e.Code, e.Message, e.WrappedErr)
	}

	return fmt.Sprintf("Error: %s (error code: %d), Message: %v, Wrapped err: %v, Data: %s", e.Code.Enum(), e.Code, e.Message, e.WrappedErr, dataMsg)
}

// Is reports whether error codes match.
func (e *Error) Is(target *Error) bool {
	if e == nil {
		return false
	}

	if e.Code == target.Code {
		return true
	}

	if e.WrappedErr == nil {
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
		returnErr := &Error{
			Code:    code,
			Message: "invalid error code",
		}
		if wErr != nil {
			returnErr.WrappedErr = wErr
		}
		return returnErr
	}

	returnErr := &Error{
		Code:    code,
		Message: message,
		//WrappedErr: wErr,
	}
	if wErr != nil {
		returnErr.WrappedErr = wErr
	}

	return returnErr
}

func WrapGRPC(err error) *Error {
	if err == nil {
		return nil
	}

	if castedErr, ok := err.(*Error); ok {
		var wrappedErrDetails []protoadapt.MessageV1
		// If the error is already a *Error, wrap it with gRPC details
		details, _ := anypb.New(&TError{
			Code:    castedErr.Code,
			Message: castedErr.Message,
		})
		wrappedErrDetails = append(wrappedErrDetails, details)
		// add every wrapped error one by one
		// var errors []*Error
		//errors = append(errors, err)
		if castedErr.WrappedErr != nil {
			currWrappedErr := castedErr.WrappedErr
			for currWrappedErr != nil {
				if err, ok := currWrappedErr.(*Error); ok {
					details, _ := anypb.New(&TError{
						Code:    err.Code,
						Message: err.Message,
					})
					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = err.WrappedErr
				} else if err, ok := currWrappedErr.(error); ok {
					details, _ := anypb.New(&TError{
						Code:    ERR_ERROR,
						Message: err.Error(),
					})
					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = nil
				}
			}
		}
		//for i := 0; i < len(wrappedErrDetails); i++ {
		//	fmt.Println("Details for the error is: ", wrappedErrDetails[i])
		//}

		st := status.New(ErrorCodeToGRPCCode(castedErr.Code), castedErr.Message)
		st, detailsErr := st.WithDetails(wrappedErrDetails...)
		if detailsErr != nil {
			// the following should not be used.
			return &Error{
				// TODO: add grpc construction error type
				Code:       ERR_ERROR,
				Message:    "error adding details to the error's gRPC status",
				WrappedErr: err,
			}
		}

		return &Error{
			Code:       castedErr.Code,
			Message:    castedErr.Message,
			WrappedErr: st.Err(),
		}
	}

	st := status.New(ErrorCodeToGRPCCode(ErrUnknown.Code), ErrUnknown.Message)
	details, _ := anypb.New(&TError{
		Code:    ERR_ERROR,
		Message: err.Error(),
	})
	st, detailsErr := st.WithDetails(details)
	if detailsErr != nil {
		// the following should not be used.
		return &Error{
			// TODO: add grpc construction error type
			Code:       ERR_ERROR,
			Message:    "error adding details to the error's gRPC status",
			WrappedErr: err,
		}
	}

	return &Error{
		Code:       ERR_ERROR,
		Message:    err.Error(),
		WrappedErr: st.Err(),
	}
}

func UnwrapGRPC(err error) *Error {
	if err == nil {
		return nil
	}

	if castedErr, ok := err.(*Error); ok {
		st, ok := status.FromError(castedErr.WrappedErr)
		if !ok {
			return castedErr // Not a gRPC status error
		}
		var prevErr *Error
		var currErr *Error
		//var toBeWrappedErr *Error
		for i := len(st.Details()) - 1; i >= 0; i-- {
			// Cast the protoadapt.MessageV1 to *anypb.Any, which is what we need to unmarshal
			detail := st.Details()[i]
			detailAny, ok := detail.(*anypb.Any)
			if !ok {
				fmt.Println("This should not happen, detail is not of type anypb.Any")
				continue // If the detail isn't of the expected type, skip it
			}

			var customDetails TError
			if err := anypb.UnmarshalTo(detailAny, &customDetails, proto.UnmarshalOptions{}); err == nil {
				currErr = New(customDetails.Code, customDetails.Message)
				// if we moved up higher in the hierarchy
				if prevErr != nil {
					currErr.WrappedErr = prevErr
				}
				prevErr = currErr
			}
		}

		return currErr
	}
	// If the error is not an "*Error", but "error", unwrap details
	st, ok := status.FromError(err)
	if !ok {
		//return err // Not a gRPC status error
		return &Error{
			Code:       ERR_ERROR,
			Message:    "error unwrapping gRPC details",
			WrappedErr: err,
		}
	}

	return &Error{
		Code:       ERR_ERROR,
		Message:    err.Error(),
		WrappedErr: st.Err(),
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
