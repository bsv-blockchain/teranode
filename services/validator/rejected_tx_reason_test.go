package validator

import (
	"io"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// The rejected-tx reason is re-broadcast to every mesh peer, so it must be a
// bounded code list carrying none of the (attacker-shaped) error text. The
// chain walk itself is covered in errors.CodeChain's tests; this pins the
// joining and the fallback.
func TestRejectedTxReason(t *testing.T) {
	inputShaped := "script failed: OP_RETURN <attacker controlled blob> " + strings.Repeat("x", 4096)

	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "single code drops the message text",
			err:  errors.NewTxInvalidError(inputShaped),
			want: "TX_INVALID",
		},
		{
			name: "chain joined outermost first",
			err:  errors.NewTxInvalidError("outer", errors.NewProcessingError("inner")),
			want: "TX_INVALID/PROCESSING",
		},
		{
			name: "no teranode code falls back to the rejection condition",
			err:  io.EOF,
			want: "TX_INVALID",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rejectedTxReason(tc.err)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "attacker", "reason must not carry error text")
		})
	}
}
