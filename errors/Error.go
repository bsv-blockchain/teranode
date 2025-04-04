package errors

import (
	"errors"
	"fmt"
	reflect "reflect"
	"runtime"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/anypb"
)

type Error struct {
	code       ERR
	message    string
	file       string
	line       int
	function   string
	wrappedErr error
	data       ErrDataI
}

type Interface interface {
	Error() string
	Is(target error) bool
	As(target interface{}) bool
	Unwrap() error

	Code() ERR
	Message() string
	WrappedErr() error
	Data() ErrDataI
}

func (e *Error) Error() string {
	// Error() can be called on wrapped errors, which can be nil, for example predefined errors
	if e == nil {
		return "<nil>"
	}

	dataMsg := ""
	if e.Data() != nil {
		dataMsg = e.data.Error()
	}

	if e.WrappedErr() == nil {
		if dataMsg == "" {
			return fmt.Sprintf("%s (%d): %v", e.code.Enum(), e.code, e.message)
		}

		return fmt.Sprintf("%d: %v %q", e.code, e.message, dataMsg)
	}

	if dataMsg == "" {
		return fmt.Sprintf("%s (%d): %v -> %v", e.code.Enum(), e.code, e.message, e.wrappedErr)
	}

	return fmt.Sprintf("%s (%d): %v -> %v %q", e.code.Enum(), e.code, e.message, e.wrappedErr, dataMsg)
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

func (e *Error) SetWrappedErr(err error) {
	if e == nil {
		return
	}

	// find the last wrapper error in the chain and set the new error as the wrapped error
	var lastWrappedErr *Error

	lastErr := e

	for lastErr.wrappedErr != nil {
		if errors.As(lastErr.wrappedErr, &lastWrappedErr) {
			lastErr = lastWrappedErr
		} else {
			// this will set lastErr.wrappedErr to nil
			lastErr = NewError(lastWrappedErr.Error())
		}
	}

	lastErr.wrappedErr = err
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

func (e *Error) Data() ErrDataI {
	if e == nil {
		return nil
	}

	return e.data
}

func (e *Error) SetData(key string, value interface{}) {
	if e.data == nil {
		e.data = &ErrData{}
	}

	var data *ErrData
	if errors.As(e.data, &data) {
		data.SetData(key, value)
	}
}

func (e *Error) GetData(key string) interface{} {
	if e.data == nil {
		return nil
	}

	return e.data.GetData(key)
}

func New(code ERR, message string, params ...interface{}) *Error {
	var wErr *Error

	// Extract the wrapped error, if present
	if len(params) > 0 {
		lastParam := params[len(params)-1]

		switch err := lastParam.(type) {
		case *Error:
			wErr = err
			params = params[:len(params)-1]
		case error:
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

	pc, file, line, _ := runtime.Caller(2)
	fn := runtime.FuncForPC(pc)
	parts := strings.Split(fn.Name(), "/")

	// Check if the code exists in the ErrorConstants enum
	if _, ok := ERR_name[int32(code)]; !ok {
		returnErr := &Error{
			code:     code,
			message:  "invalid error code",
			file:     file,
			line:     line,
			function: parts[len(parts)-1],
		}
		if wErr != nil {
			returnErr.wrappedErr = wErr
		}

		return returnErr
	}

	returnErr := &Error{
		code:     code,
		message:  message,
		file:     file,
		line:     line,
		function: parts[len(parts)-1],
	}

	if wErr != nil {
		returnErr.wrappedErr = wErr
	}

	return returnErr
}

func (x *TError) Error() string {
	if x.IsNil() {
		return "<nil>"
	}

	if x.WrappedError == nil {
		return fmt.Sprintf("%s (%d): %s", x.Code.String(), x.Code, x.Message)
	}

	return fmt.Sprintf("%s (%d): %s -> %v", x.Code.String(), x.Code, x.Message, x.WrappedError)
}

func (x *TError) IsNil() bool {
	if x == nil || (x.Code == ERR_UNKNOWN && x.Message == "") {
		return true
	}

	return false
}

func Wrap(err error) *TError {
	if err == nil {
		return nil
	}

	if castError, ok := err.(*Error); ok {
		return &TError{
			Code:         castError.code,
			Message:      RemoveInvalidUTF8(castError.message),
			WrappedError: Wrap(castError.wrappedErr),
			File:         castError.file,
			Line:         int32(castError.line), // nolint:gosec
			Function:     castError.function,
		}
	}

	return &TError{
		Code:    ERR_UNKNOWN,
		Message: RemoveInvalidUTF8(err.Error()),
	}
}

// WrapGRPC wraps an error with gRPC status details.
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

		var pbError error

		// If the error is already an *Error, wrap it with gRPC details
		var details protoadapt.MessageV1
		if castedErr.data != nil {
			details, pbError = anypb.New(&TError{
				Code:     castedErr.code,
				Message:  castedErr.message,
				Data:     castedErr.data.EncodeErrorData(),
				File:     castedErr.file,
				Line:     int32(castedErr.line), // nolint:gosec
				Function: castedErr.function,
			})
		} else {
			details, pbError = anypb.New(&TError{
				Code:     castedErr.code,
				Message:  castedErr.message,
				File:     castedErr.file,
				Line:     int32(castedErr.line), // nolint:gosec
				Function: castedErr.function,
			})
		}

		if pbError != nil {
			pc, file, line, _ := runtime.Caller(1)
			fn := runtime.FuncForPC(pc)
			parts := strings.Split(fn.Name(), "/")

			err2 := &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error serializing TError to protobuf Any",
				file:       file,
				line:       line,
				function:   parts[len(parts)-1],
				wrappedErr: err,
			}

			return err2
		}

		wrappedErrDetails = append(wrappedErrDetails, details)

		if castedErr.wrappedErr != nil {
			currWrappedErr := castedErr.wrappedErr
			for currWrappedErr != nil {
				if err, ok := currWrappedErr.(*Error); ok {
					var details protoadapt.MessageV1

					var pbError error

					if err.data != nil {
						details, pbError = anypb.New(&TError{
							Code:     err.code,
							Message:  err.message,
							Data:     err.data.EncodeErrorData(),
							File:     err.file,
							Line:     int32(err.line), // nolint:gosec
							Function: err.function,
						})
					} else {
						details, pbError = anypb.New(&TError{
							Code:     err.code,
							Message:  err.message,
							File:     err.file,
							Line:     int32(err.line), // nolint:gosec
							Function: err.function,
						})
					}

					if pbError != nil {
						pc, file, line, _ := runtime.Caller(1)
						fn := runtime.FuncForPC(pc)
						parts := strings.Split(fn.Name(), "/")

						err2 := &Error{
							// TODO: add grpc construction error type
							code:       ERR_ERROR,
							message:    "error serializing TError to protobuf Any",
							file:       file,
							line:       line,
							function:   parts[len(parts)-1],
							wrappedErr: err,
						}

						return err2
					}

					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = err.wrappedErr
				} else {
					pc, file, line, _ := runtime.Caller(1)
					fn := runtime.FuncForPC(pc)
					parts := strings.Split(fn.Name(), "/")

					details, _ := anypb.New(&TError{
						Code:     ERR_ERROR,
						Message:  err.Error(),
						File:     file,
						Line:     int32(line), // nolint:gosec
						Function: parts[len(parts)-1],
					})
					wrappedErrDetails = append(wrappedErrDetails, details)
					currWrappedErr = nil
				}
			}
		}

		st := status.New(ErrorCodeToGRPCCode(castedErr.code), castedErr.message)
		st, detailsErr := st.WithDetails(wrappedErrDetails...)

		if detailsErr != nil {
			pc, file, line, _ := runtime.Caller(1)
			fn := runtime.FuncForPC(pc)
			parts := strings.Split(fn.Name(), "/")

			err2 := &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error adding details to the error's gRPC status",
				file:       file,
				line:       line,
				function:   parts[len(parts)-1],
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

func UnwrapGRPC(err error) *Error {
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

			if customDetails.Data != nil {
				// get the data
				data, errorGettingData := GetErrorData(customDetails.Code, customDetails.Data)
				if errorGettingData != nil {
					// TODO (GOKHAN) CHECK OPTION OF LOGGING / PRINTING HERE
					currErr.data = nil
				} else {
					currErr.data = data
				}
			}

			// Set file, line and function information from the TError
			currErr.file = customDetails.File
			currErr.line = int(customDetails.Line)
			currErr.function = customDetails.Function

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

func AsData(err error, target interface{}) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	// cycle through the wrapped errors and check if any of them match the target
	if castedErr, ok := err.(*Error); ok {
		if errors.As(castedErr.data, target) {
			return true
		}

		if castedErr.wrappedErr != nil {
			return AsData(castedErr.wrappedErr, target)
		}
	}

	return false
}

func As(err error, target any) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	// cycle through the wrapped errors and check if any of them match the target
	if castedErr, ok := err.(*Error); ok {
		if castedErr.As(target) {
			return true
		}

		if castedErr != nil && castedErr.wrappedErr != nil {
			return errors.As(castedErr.wrappedErr, target)
		}
	}

	return errors.As(err, target)
}

func isGRPCWrappedError(err error) bool {
	_, ok := status.FromError(err)
	return ok
}

// buildStackTrace returns just the stack trace portion of the error message
func (e *Error) buildStackTrace() string {
	trace := fmt.Sprintf("\n- %s%s() %s:%d [%d] %s", "", e.function, e.file, e.line, e.code, e.message)

	if e.wrappedErr != nil {
		if werr, ok := e.wrappedErr.(*Error); ok && werr.Code() != ERR_UNKNOWN {
			trace += werr.buildStackTrace()
		} else {
			trace += fmt.Sprintf("\n- %v", e.wrappedErr)
		}
	}

	return trace
}

// Format implements fmt.Formatter for custom formatting
func (e *Error) Format(f fmt.State, c rune) {
	msg := e.Error()
	if c == 'v' && (f.Flag('+') || f.Flag('#')) {
		msg += e.buildStackTrace()
	}

	fmt.Fprint(f, msg)
}

// RemoveInvalidUTF8 sanitizes a string by removing invalid UTF-8 characters.
// This is used to clean error messages before sending them to clients.
//
// Parameters:
//   - s: string to sanitize
//
// Returns:
//   - string: sanitized string with valid UTF-8 characters only
func RemoveInvalidUTF8(s string) string {
	var buf = make([]rune, 0, len(s))

	for _, r := range s {
		if r == utf8.RuneError {
			continue
		}

		buf = append(buf, r)
	}

	return string(buf)
}
