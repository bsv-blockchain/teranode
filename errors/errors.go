// Package errors provides a comprehensive framework for defining, handling, and propagating errors within the Teranode project.
//
// This package includes:
// - Custom error types with additional context, such as file, line, and function information.
// - Methods for wrapping and unwrapping errors to maintain error chains.
// - Integration with gRPC for error serialization and deserialization.
// - Support for structured error data to provide detailed information about specific error cases.
//
// The errors package is designed to enhance debugging and error reporting by providing rich contextual information.
// It is used throughout the Teranode project to ensure consistent error handling and propagation.
package errors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/anypb"
)

// Error represents a custom error type with additional context.
type Error struct {
	code       ERR
	message    string
	file       string
	line       int
	function   string
	wrappedErr error
	data       ErrDataI
}

// Interface defines the methods that the Error type must implement to be compatible with the standard error interface.
type Interface interface {
	As(target interface{}) bool
	Code() ERR
	Data() ErrDataI
	Error() string
	Is(target error) bool
	Message() string
	Unwrap() error
	WrappedErr() error
}

// Error returns the string representation of the error.
func (e *Error) Error() string {
	// Error() can be called on wrapped errors, which can be nil, for example, predefined errors
	if e == nil {
		return "<nil>"
	}

	// Use strings.Builder for efficient string concatenation
	var b strings.Builder
	// Pre-allocate reasonable capacity to reduce allocations
	b.Grow(256)

	// Get data message if it exists
	dataMsg := ""
	if e.Data() != nil {
		dataMsg = e.data.Error()
	}

	// If the wrapped error is nil, return the error message without the wrapped error
	if e.WrappedErr() == nil {
		if dataMsg == "" {
			b.WriteString(e.code.String())
			b.WriteString(" (")
			b.WriteString(fmt.Sprint(int32(e.code)))
			b.WriteString("): ")
			b.WriteString(e.message)
		} else {
			// Special format when data but no wrapped error: just number
			b.WriteString(fmt.Sprint(int32(e.code)))
			b.WriteString(": ")
			b.WriteString(e.message)
			b.WriteString(" \"")
			b.WriteString(dataMsg)
			b.WriteString("\"")
		}
		return b.String()
	}

	// Has wrapped error - use full format with name
	b.WriteString(e.code.String())
	b.WriteString(" (")
	b.WriteString(fmt.Sprint(int32(e.code)))
	b.WriteString("): ")
	b.WriteString(e.message)

	// Add data message if present
	if dataMsg != "" {
		b.WriteString(" \"")
		b.WriteString(dataMsg)
		b.WriteString("\"")
	}

	// Add wrapped error chain - use iterative approach to avoid recursive Error() calls
	if e.wrappedErr != nil {
		b.WriteString(" -> ")
		// Limit chain depth to prevent excessive formatting
		depth := 0
		maxDepth := 20
		current := e.wrappedErr

		for current != nil && depth < maxDepth {
			if errPtr, ok := current.(*Error); ok {
				b.WriteString(errPtr.code.String())
				b.WriteString(" (")
				b.WriteString(fmt.Sprint(int32(errPtr.code)))
				b.WriteString("): ")
				b.WriteString(errPtr.message)

				if errPtr.Data() != nil {
					dataMsg := errPtr.data.Error()
					if dataMsg != "" {
						b.WriteString(" \"")
						b.WriteString(dataMsg)
						b.WriteString("\"")
					}
				}

				current = errPtr.wrappedErr
				if current != nil {
					b.WriteString(" -> ")
				}
			} else {
				// Non-Error type, use Error() method
				b.WriteString(current.Error())
				break
			}
			depth++
		}

		if depth >= maxDepth {
			b.WriteString("... (chain truncated at ")
			b.WriteString(fmt.Sprint(maxDepth))
			b.WriteString(" errors)")
		}
	}

	return b.String()
}

// Is reports whether error codes match.
func (e *Error) Is(target error) bool {
	if e == nil || target == nil {
		return false
	}

	var targetError *Error
	ok := errors.As(target, &targetError)
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
		var ue *Error
		if errors.As(unwrapped, &ue) {
			return ue.Is(target)
		}
	}

	return false
}

// As attempts to assign the error to the target if types are compatible.
func (e *Error) As(target interface{}) bool {
	if e == nil {
		return false
	}

	// Fast path for our own error type.
	if te, ok := target.(**Error); ok {
		*te = e
		return true
	}

	// Try the data payload (not reachable via Unwrap, so check it here).
	if err, ok := e.data.(error); ok {
		if as, ok := err.(interface{ As(interface{}) bool }); ok && as.As(target) {
			return true
		}
	}

	// Try wrapped error, but ONLY if it's not an *Error type.
	// If wrapped error is *Error, skip here - Go's errors.As will find it via Unwrap().
	// This prevents infinite recursion when *Error wraps *Error.
	if e.wrappedErr != nil {
		// Skip if wrapped error is *Error to avoid recursive loop
		if _, isError := e.wrappedErr.(*Error); !isError {
			if as, ok := e.wrappedErr.(interface{ As(interface{}) bool }); ok && as.As(target) {
				return true
			}
		}
	}

	return false
}

// SetWrappedErr sets the wrapped error in the error chain.
func (e *Error) SetWrappedErr(err error) {
	if e == nil || err == nil {
		return
	}

	// Prevent self-referencing
	if errPtr, ok := err.(*Error); ok && errPtr == e {
		return
	}

	// Check if adding err would create a cycle
	if errPtr, ok := err.(*Error); ok && errPtr.contains(e) {
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

	// Final check: don't add if err already exists in our chain
	if errPtr, ok := err.(*Error); ok && e.contains(errPtr) {
		return
	}

	lastErr.wrappedErr = err
}

// Unwrap returns the wrapped error if present.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}

	return e.wrappedErr
}

// Code returns the error code.
func (e *Error) Code() ERR {
	if e == nil {
		return ERR_UNKNOWN
	}

	return e.code
}

// Message returns the error message.
func (e *Error) Message() string {
	if e == nil {
		return ""
	}

	return e.message
}

// WrappedErr returns the wrapped error if present.
func (e *Error) WrappedErr() error {
	if e == nil {
		return nil
	}

	return e.wrappedErr
}

// Data retrieves the error data associated with the error.
func (e *Error) Data() ErrDataI {
	if e == nil {
		return nil
	}

	return e.data
}

// SetData sets a key-value pair in the error data.
func (e *Error) SetData(key string, value interface{}) {
	if e.data == nil {
		e.data = &ErrData{}
	}

	var data *ErrData
	if errors.As(e.data, &data) {
		data.SetData(key, value)
	}
}

// GetData retrieves the value associated with a key in the error data.
func (e *Error) GetData(key string) interface{} {
	if e == nil || e.data == nil {
		return nil
	}
	return e.data.GetData(key)
}

// New creates a new Error instance with the specified code, message, and optional parameters.
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
		//nolint:forbidigo // Ignore this linter warning, this is an exception where we want to use fmt.Errorf
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
		// Only wrap if wErr exists and won't create a cycle
		if wErr != nil && wErr != returnErr && !wErr.contains(returnErr) {
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

	// Only wrap if wErr exists and won't create a cycle
	if wErr != nil && wErr != returnErr && !wErr.contains(returnErr) {
		returnErr.wrappedErr = wErr
	}

	return returnErr
}

// contains checks if the target error or any of its wrapped errors exist in this error's chain
func (e *Error) contains(target *Error) bool {
	if e == nil || target == nil {
		return false
	}

	// Build a set of all errors in e's chain using struct{} for zero memory overhead
	// Pre-allocate with capacity to reduce allocations for typical chain depths
	eChain := make(map[*Error]struct{}, 8)

	current := e
	for current != nil {
		// Check for cycles while building the chain
		if _, exists := eChain[current]; exists {
			break // Cycle detected, stop traversal
		}
		eChain[current] = struct{}{}

		// Move to the next wrapped error
		if wrappedErr, ok := current.wrappedErr.(*Error); ok {
			current = wrappedErr
		} else {
			break
		}
	}

	// Check if any error in target's chain exists in e's chain
	// Track visited errors in target's chain to detect cycles
	visited := make(map[*Error]struct{}, 4)
	current = target
	for current != nil {
		if _, exists := visited[current]; exists {
			return false // Cycle detected in target chain
		}
		visited[current] = struct{}{}

		// Check if this target error exists in e's chain
		if _, exists := eChain[current]; exists {
			return true
		}

		// Move to the next wrapped error in target's chain
		if wrappedErr, ok := current.wrappedErr.(*Error); ok {
			current = wrappedErr
		} else {
			break
		}
	}

	return false
}

// Error represents a custom error type that implements the error interface.
func (x *TError) Error() string {
	if x.IsNil() {
		return "<nil>"
	}

	if x.WrappedError == nil {
		return fmt.Sprintf("%s (%d): %s", x.Code.String(), x.Code, x.Message)
	}

	return fmt.Sprintf("%s (%d): %s -> %v", x.Code.String(), x.Code, x.Message, x.WrappedError)
}

// IsNil checks if the TError is nil or has no meaningful content.
func (x *TError) IsNil() bool {
	if x == nil || (x.Code == ERR_UNKNOWN && x.Message == "") {
		return true
	}

	return false
}

func (x *TError) Unwrap() error {
	if x.WrappedError == nil {
		return nil
	}

	return x.WrappedError
}

func (x *TError) Is(target error) bool {
	if x == nil {
		return false
	}

	if target == nil {
		return false
	}

	targetTError, ok := target.(*TError)
	if ok && x.Code == targetTError.Code {
		return true
	}

	targetError, ok := target.(*Error)
	if ok && x.Code == targetError.Code() {
		return true
	}

	return false
}

// Wrap converts a standard error into a TError, preserving the error code and message.
func Wrap(err error) *TError {
	if err == nil {
		return nil
	}

	var castError *Error
	if errors.As(err, &castError) {
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
	var castedErr *Error
	if errors.As(err, &castedErr) {
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

			return &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error serializing TError to protobuf Any",
				file:       file,
				line:       line,
				function:   parts[len(parts)-1],
				wrappedErr: err,
			}
		}

		wrappedErrDetails = append(wrappedErrDetails, details)

		if castedErr.wrappedErr != nil {
			currWrappedErr := castedErr.wrappedErr
			for currWrappedErr != nil {
				var err *Error
				if errors.As(currWrappedErr, &err) {
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
				}
			}
		}

		st := status.New(ErrorCodeToGRPCCode(castedErr.code), castedErr.message)
		st, detailsErr := st.WithDetails(wrappedErrDetails...)

		if detailsErr != nil {
			pc, file, line, _ := runtime.Caller(1)
			fn := runtime.FuncForPC(pc)
			parts := strings.Split(fn.Name(), "/")

			return &Error{
				// TODO: add grpc construction error type
				code:       ERR_ERROR,
				message:    "error adding details to the error's gRPC status",
				file:       file,
				line:       line,
				function:   parts[len(parts)-1],
				wrappedErr: err,
			}
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

// UserMessage returns a concise, user-facing error message without wrapped error chains or data.
// It is intended for external surfaces (HTTP/gRPC) where internal details should not be exposed.
func UserMessage(err error) string {
	if err == nil {
		return ""
	}

	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	var tErr *Error
	if errors.As(err, &tErr) && tErr != nil {
		return fmt.Sprintf("%s (%d): %s", tErr.code.String(), tErr.code, tErr.message)
	}

	var tProtoErr *TError
	if errors.As(err, &tProtoErr) && tProtoErr != nil {
		return fmt.Sprintf("%s (%d): %s", tProtoErr.Code.String(), tProtoErr.Code, tProtoErr.Message)
	}

	return "internal error"
}

// PublicError returns a sanitized Error suitable for returning to external callers.
// It strips wrapped errors, file/line/function details, and error data.
func PublicError(err error) *Error {
	if err == nil {
		return nil
	}

	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	var tErr *Error
	if errors.As(err, &tErr) && tErr != nil {
		return &Error{
			code:    tErr.code,
			message: tErr.message,
		}
	}

	var tProtoErr *TError
	if errors.As(err, &tProtoErr) && tProtoErr != nil {
		return &Error{
			code:    tProtoErr.Code,
			message: tProtoErr.Message,
		}
	}

	return &Error{
		code:    ERR_ERROR,
		message: "internal error",
	}
}

// WrapGRPCPublic wraps an error for gRPC without exposing internal details.
func WrapGRPCPublic(err error) error {
	if err == nil {
		return nil
	}

	publicErr := PublicError(err)
	if publicErr == nil {
		return nil
	}

	st := status.New(ErrorCodeToGRPCCode(publicErr.code), publicErr.message)
	return st.Err()
}

// WrapPublic converts an error into a TError without wrapped details for external use.
func WrapPublic(err error) *TError {
	if err == nil {
		return nil
	}

	return Wrap(PublicError(err))
}

// UnwrapGRPC unwraps a gRPC error and returns the underlying Error type.
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
		if err = anypb.UnmarshalTo(detailAny, &customDetails, proto.UnmarshalOptions{}); err == nil {
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

// Join combines multiple errors into a single error.
func Join(errs ...error) error {
	var (
		messages    []string
		tErr        *Error
		tErr2       *Error
		previousErr *Error
	)

	// If the first error is a custom Error type, chain the rest as wrapped errors.
	if len(errs) > 0 && errors.As(errs[0], &tErr) {
		previousErr = tErr

		for i := 1; i < len(errs); i++ {
			if errors.As(errs[i], &tErr2) {
				previousErr.SetWrappedErr(tErr2)
			} else {
				tErr2 = NewError(errs[i].Error())
				previousErr.SetWrappedErr(tErr2)
			}

			previousErr = tErr2
		}

		return tErr
	}

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

// Is checks if the error matches the target error.
func Is(err, target error) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	return errors.Is(err, target)
}

// AsData attempts to assign the error data to the target if types are compatible.
func AsData(err error, target interface{}) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	// cycle through the wrapped errors and check if any of them match the target
	var castedErr *Error
	if errors.As(err, &castedErr) {
		if errors.As(castedErr.data, target) {
			return true
		}

		if castedErr.wrappedErr != nil {
			return AsData(castedErr.wrappedErr, target)
		}
	}

	return false
}

// As attempts to assign the error to the target if types are compatible.
func As(err error, target any) bool {
	if isGRPCWrappedError(err) {
		err = UnwrapGRPC(err)
	}

	// cycle through the wrapped errors and check if any of them match the target
	var castedErr *Error
	if errors.As(err, &castedErr) {
		if castedErr.As(target) {
			return true
		}

		if castedErr != nil && castedErr.wrappedErr != nil {
			return errors.As(castedErr.wrappedErr, target)
		}
	}

	return errors.As(err, target)
}

// isGRPCWrappedError checks if the error is a gRPC wrapped error.
func isGRPCWrappedError(err error) bool {
	_, ok := status.FromError(err)
	return ok
}

// buildStackTrace returns just the stack trace portion of the error message.
func (e *Error) buildStackTrace() string {
	trace := fmt.Sprintf("\n- %s%s() %s:%d [%d] %s", "", e.function, e.file, e.line, e.code, e.message)

	if e.wrappedErr != nil {
		var wrappedErr *Error
		if errors.As(e.wrappedErr, &wrappedErr) && wrappedErr.Code() != ERR_UNKNOWN {
			trace += wrappedErr.buildStackTrace()
		}
	}

	return trace
}

// Format implements fmt.Formatter for custom formatting.
func (e *Error) Format(f fmt.State, c rune) {
	msg := e.Error()
	if c == 'v' && (f.Flag('+') || f.Flag('#')) {
		msg += e.buildStackTrace()
	}

	_, _ = fmt.Fprint(f, msg)
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
