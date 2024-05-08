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
}

func (e *Error) Error() string {
	if e.WrappedErr == nil {
		return fmt.Sprintf("%d: %v", e.Code, e.Message)
	}

	return fmt.Sprintf("Error: %s (error code: %d),  %v: %v", e.Code.Enum(), e.Code, e.Message, e.WrappedErr)
}

// Is reports whether error codes match.
func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}

	var ue *Error
	if errors.As(target, &ue) {
		//return e.Code == ue.Code
		if e.Code == ue.Code {
			return true
		}

		if e.WrappedErr == nil {
			return false
		}
	}

	// Unwrap the current error and recursively call Is on the unwrapped error
	if unwrapped := errors.Unwrap(e); unwrapped != nil {
		if ue, ok := unwrapped.(*Error); ok {
			return ue.Is(target)
		}
	}

	return false
}

func (e *Error) Unwrap() error {
	return e.WrappedErr
}

// type ErrDataKey struct {
// 	key string
// }

// type ErrData interface {
// 	Error() string
// }

// type ErrDataUtxoSpent struct {
// 	Hash           chainhash.Hash
// 	SpendingTxHash chainhash.Hash
// 	Time           time.Time
// }

// func (e *ErrDataUtxoSpent) Error() string {
// 	return fmt.Sprintf("utxo %s already spent by %s at %s", e.Hash, e.SpendingTxHash, e.Time)
// }

// func UtxoSpent(txID chainhash.Hash, spendingTxID chainhash.Hash, t time.Time, err error) error {
// 	utxoSpentErr := &ErrDataUtxoSpent{
// 		Hash:           txID,
// 		SpendingTxHash: spendingTxID,
// 		Time:           t,
// 	}
// 	e := Join(utxoSpentErr, err)
// 	return New(ERR_TX_ALREADY_EXISTS, utxoSpentErr.Error(), e)
// }

// func (e *Error) Asssss(err error) bool {
// 	//errrrr := New(ERR_TX_ALREADY_EXISTS, "utxo already spent: %s", "string", UtxoSpentData(chainhash.Hash{}), err)
// 	errrrr := UtxoSpent(chainhash.Hash{}, chainhash.Hash{}, time.Now(), err)

// 	var spentErr *DataUtxoSpent
// 	if errors.As(errrrr, &spentErr) {
// 		if spentErr.SpendingTxHash.Equal(chainhash.Hash{}) {
// 			return true
// 		}
// 	}

// 	return false
// }

func New(code ERR, message string, params ...interface{}) *Error {
	var wErr *Error
	//var data map[string]interface{}

	// Extract the wrapped error and data, if present
	if len(params) > 0 {
		if err, ok := params[len(params)-1].(*Error); ok {
			wErr = err
			//data = err.Data
			params = params[:len(params)-1]
		}
	}

	// Extract additional data, if present
	// if len(params)%2 == 0 {
	// if data == nil {
	// 	data = make(map[string]interface{})
	// }
	// for i := 0; i < len(params); i += 2 {
	// 	if key, ok := params[i].(string); ok {
	// 		data[key] = params[i+1]
	// 	}
	// }
	// }

	// Format the message with the remaining parameters
	if len(params) > 0 {
		message = fmt.Sprintf(message, params...)
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

// func (e *Error) WithData(key string, value interface{}) *Error {
// 	//e.Data = append(e.Data, key)

// 	return e
// }

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
	return errors.As(err, target)
}
