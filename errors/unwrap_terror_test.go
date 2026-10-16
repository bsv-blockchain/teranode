package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// links walks an *Error chain outermost first, returning each link's code and
// message.
func links(t *testing.T, e *Error) ([]ERR, []string) {
	t.Helper()

	codes := []ERR{}
	messages := []string{}

	for cur := e; cur != nil; {
		codes = append(codes, cur.Code())
		messages = append(messages, cur.Message())

		next, ok := cur.WrappedErr().(*Error)
		if !ok {
			break
		}

		cur = next
	}

	return codes, messages
}

func TestUnwrapTError_RebuildsTheChainInOrder(t *testing.T) {
	original := NewProcessingError("[Validate][%s] error validating transaction", "abcd",
		NewTxInvalidError("GoBDK fail to ValidateTransaction",
			New(ERR_TX_INVALID, "bad-txns-inputs-duplicate")))

	got := UnwrapTError(Wrap(original))
	require.NotNil(t, got)

	codes, messages := links(t, got)
	require.Equal(t, []ERR{ERR_PROCESSING, ERR_TX_INVALID, ERR_TX_INVALID}, codes)
	require.Equal(t, []string{
		"[Validate][abcd] error validating transaction",
		"GoBDK fail to ValidateTransaction",
		"bad-txns-inputs-duplicate",
	}, messages)

	require.ErrorIs(t, got, ErrTxInvalid, "class checks see through the converted chain")
	require.Equal(t, original.file, got.file, "source location is carried across")
	require.Equal(t, original.line, got.line)
}

func TestUnwrapTError_AgreesWithTheGRPCRoute(t *testing.T) {
	// One conversion, two entry points: a status's details and a bare *TError must
	// rebuild the same chain.
	original := NewServiceError("[RevalidateBlock][%s] failed block re-validation", "0abc",
		NewBlockInvalidError("[ValidateBlock][0abc] bad-blk-length"))

	viaTError := UnwrapTError(Wrap(original))
	viaGRPC := UnwrapGRPC(WrapGRPC(original))

	tCodes, tMessages := links(t, viaTError)
	gCodes, gMessages := links(t, viaGRPC)

	require.Equal(t, gCodes, tCodes)
	require.Equal(t, gMessages, tMessages)
}

func TestUnwrapTError_KeepsAPercentInAMessage(t *testing.T) {
	// The message is data, not a format string. A producer's text containing a
	// verb must come back as written.
	got := UnwrapTError(&TError{Code: ERR_TX_POLICY, Message: "fee rate 50% below minimum %s"})
	require.NotNil(t, got)
	require.Equal(t, "fee rate 50% below minimum %s", got.Message())
}

func TestUnwrapTError_NilAndEmptyAreNil(t *testing.T) {
	require.Nil(t, UnwrapTError(nil))
	require.Nil(t, UnwrapTError(&TError{}), "an empty TError is how the batch response says 'no error'")
}
