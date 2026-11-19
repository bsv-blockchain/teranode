package validator

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestNonDefaultValidationOptions_CoversEveryWireField turns the guard's
// MAINTENANCE CONTRACT from "someone remembered" into a build failure.
//
// nonDefaultValidationOptions enumerates the fields of ValidateTransactionRequest
// by hand, a second copy of what optionsFromValidateRequest already lists, with no
// compile-time link to the proto. It is complete today; the next field wired into
// the projection without a matching branch here becomes a silent hole in the
// guard — the same hole issue 4840 exists to close.
//
// So walk the descriptor instead of trusting a list: for every wire-settable field
// except the transaction payload, set it to a non-default value and require the
// guard to name it.
func TestNonDefaultValidationOptions_CoversEveryWireField(t *testing.T) {
	fields := (&validator_api.ValidateTransactionRequest{}).ProtoReflect().Descriptor().Fields()

	covered := 0

	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)

		// The payload the transport exists to carry, and the only deliberate
		// omission from the guard.
		if fd.Name() == "transaction_data" {
			continue
		}

		covered++

		t.Run(string(fd.Name()), func(t *testing.T) {
			var value protoreflect.Value

			switch fd.Kind() {
			case protoreflect.BoolKind:
				// add_tx_to_block_assembly defaults to TRUE (NewDefaultOptions), so
				// its non-default is false. Setting false through Message.Set on an
				// optional field marks presence, which is exactly what the guard's
				// "!= nil && !*" branch keys on.
				value = protoreflect.ValueOfBool(fd.Name() != "add_tx_to_block_assembly")
			case protoreflect.Uint32Kind:
				value = protoreflect.ValueOfUint32(1)
			default:
				t.Fatalf("a new field kind reached ValidateTransactionRequest: extend nonDefaultValidationOptions and this test (field %s, kind %s)", fd.Name(), fd.Kind())
			}

			req := &validator_api.ValidateTransactionRequest{}
			req.ProtoReflect().Set(fd, value)

			require.NotEmpty(t, nonDefaultValidationOptions(req),
				"field %s is settable on the wire but the guard does not reject it", fd.Name())
		})
	}

	// Without this the walk would pass vacuously on an empty or truncated
	// descriptor, which is the failure mode the test exists to prevent.
	require.Equal(t, 12, covered,
		"the guard is expected to cover 12 wire-settable fields; update this count deliberately when the proto changes")
}
