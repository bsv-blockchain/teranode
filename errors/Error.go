package errors

import (
	"errors"
	"fmt"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/anypb"
	reflect "reflect"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
func (e *Error) Is(target *Error) bool {
	if e == nil {
		return false
	}

	fmt.Println("\nChecking e: ", e.Error(), ", with code:", e.Code, "\ntarget: ", target.Error(), ", with code:", target.Code)

	//var ue *Error

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
	//details, _ := anypb.New(&TError{
	//	Code:         err.Code,
	//	Message:      err.Message,
	//	WrappedError: err.WrappedErr.Error(),
	//})

	var wrappedErrDetails []protoadapt.MessageV1
	// add every wrapped error one by one
	// var errors []*Error
	//errors = append(errors, err)
	if err.WrappedErr != nil {
		currWrappedErr := err.WrappedErr
		for currWrappedErr != nil {
			if err, ok := currWrappedErr.(*Error); ok {
				details, _ := anypb.New(&TError{
					Code:    err.Code,
					Message: err.Message,
				})
				wrappedErrDetails = append(wrappedErrDetails, details)
				// errors = append(errors, err)

				// exit the loop if the wrapped error is set to <nil>
				if err.WrappedErr.Error() == "<nil>" {
					currWrappedErr = nil
				} else {
					currWrappedErr = err.WrappedErr
				}

			} else if err, ok := currWrappedErr.(error); ok {
				details, _ := anypb.New(&TError{
					Code:    ERR_ERROR,
					Message: err.Error(),
				})
				wrappedErrDetails = append(wrappedErrDetails, details)
				//errors = append(errors, &Error{
				//	Code:    ERR_ERROR,
				//	Message: err.Error(),
				//})
				// exit loop
				currWrappedErr = nil
			}
		}
	}
	for i := 0; i < len(wrappedErrDetails); i++ {
		// fmt.Println("Adding error to the details: ", errors[i].Error())
		fmt.Println("Details for the error is: ", wrappedErrDetails[i])
	}

	st := status.New(ErrorCodeToGRPCCode(err.Code), err.Message)
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

	/*
		//	var ubsvErr TError
		var finalErr *Error
		var currErr *Error
		var ubsvErr TError
		// Attempt to extract and return detailed UBSVError if present
		if len(st.Details()) > 0 {
			// get the detail
			detail := st.Details()[0]

			if err := anypb.UnmarshalTo(detail.(*anypb.Any), &ubsvErr, proto.UnmarshalOptions{}); err == nil {
				fmt.Println("Unmarshalled ubsv error, Code: ", ubsvErr.Code, ",  Message: ", ubsvErr.Message, ", Wrapped Error:", ubsvErr.WrappedError)
				// Traverse through details, convert to errors separately
				// don't construct the error until the end

				for ubsvErr.WrappedError != "" {

				}
				//if ubsvErr.WrappedError == "" {
				lastErr := &Error{
					Code:       ubsvErr.Code,
					Message:    ubsvErr.Message,
					WrappedErr: nil,
				}
				// Add last error to the latest error chain
				//currErr.WrappedErr = lastErr

				//}

				//return New(ubsvErr.Code, ubsvErr.Message, ubsvErr.WrappedError)
			}
		}
	*/
	//fmt.Println("Starting to go through details, current detail is: ", detail)
	//fmt.Println("All details , size of details:", len(st.Details()), " details are: ", st.Details())

	//if ubsvErr.WrappedError != "" {
	//	foundErr := New(ubsvErr.Code, ubsvErr.Message, ubsvErr.WrappedError)
	//	fmt.Println("since wrapped error is not nil, returning new error with wrapped error: ", foundErr.Error())
	//	return foundErr
	//} else {
	//	return New(ubsvErr.Code, ubsvErr.Message)
	//}

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
