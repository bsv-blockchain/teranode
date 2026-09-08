package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodeChain(t *testing.T) {
	cases := []struct {
		name string
		err  error
		max  int
		want []ERR
	}{
		{
			name: "nil",
			err:  nil,
			max:  4,
			want: nil,
		},
		{
			name: "single",
			err:  NewTxInvalidError("script failed: <input-shaped detail>"),
			max:  4,
			want: []ERR{ERR_TX_INVALID},
		},
		{
			name: "outermost first",
			err:  NewTxInvalidError("outer", NewProcessingError("inner")),
			max:  4,
			want: []ERR{ERR_TX_INVALID, ERR_PROCESSING},
		},
		{
			name: "repeated codes deduplicated",
			err:  NewTxInvalidError("a", NewTxInvalidError("b", NewUtxoFrozenError("c"))),
			max:  4,
			want: []ERR{ERR_TX_INVALID, ERR_UTXO_FROZEN},
		},
		{
			name: "foreign outer link skipped not terminal",
			err:  fmt.Errorf("plain %w", NewTxInvalidError("x", NewProcessingError("deep"))),
			max:  4,
			want: []ERR{ERR_TX_INVALID, ERR_PROCESSING},
		},
		{
			// New flattens a foreign wrapped error into an ERR_UNKNOWN link
			// with only its text; that link must not surface as a code.
			name: "flattened foreign wrapped error contributes nothing",
			err:  NewTxInvalidError("wrapped", fmt.Errorf("plain")),
			max:  4,
			want: []ERR{ERR_TX_INVALID},
		},
		{
			name: "explicit unknown link skipped",
			err:  NewTxInvalidError("a", NewUnknownError("b", NewProcessingError("c"))),
			max:  4,
			want: []ERR{ERR_TX_INVALID, ERR_PROCESSING},
		},
		{
			name: "foreign only",
			err:  errors.New("foreign"),
			max:  4,
			want: nil,
		},
		{
			name: "capped",
			err: NewTxInvalidError("1",
				NewProcessingError("2",
					NewUtxoFrozenError("3",
						NewUtxoNonFinalError("4",
							NewTxInvalidDoubleSpendError("5"))))),
			max:  4,
			want: []ERR{ERR_TX_INVALID, ERR_PROCESSING, ERR_UTXO_FROZEN, ERR_UTXO_NON_FINAL},
		},
		{
			name: "no cap",
			err: NewTxInvalidError("1",
				NewProcessingError("2",
					NewUtxoFrozenError("3",
						NewUtxoNonFinalError("4",
							NewTxInvalidDoubleSpendError("5"))))),
			max:  0,
			want: []ERR{ERR_TX_INVALID, ERR_PROCESSING, ERR_UTXO_FROZEN, ERR_UTXO_NON_FINAL, ERR_TX_INVALID_DOUBLE_SPEND},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, CodeChain(tc.err, tc.max))
		})
	}
}
