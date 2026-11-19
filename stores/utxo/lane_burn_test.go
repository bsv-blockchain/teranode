package utxo

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

func TestLaneBurnReason(t *testing.T) {
	refused := []*Spend{nil, {Err: errors.NewUtxoSpentError([32]byte{}, 0, [32]byte{}, nil)}}

	cases := []struct {
		name   string
		err    error
		spends []*Spend
		want   string
	}{
		{"the transaction exists", errors.NewTxExistsError("exists"), nil, LaneBurnExists},
		{"exists wins over a refused spend", errors.NewTxExistsError("exists"), refused, LaneBurnExists},
		{"a spend refused on its record", errors.NewProcessingError("failed"), refused, LaneBurnSpendRejected},
		{"a utxo error", errors.NewUtxoError("2 of 2 inputs could not be spent"), nil, LaneBurnSpendRejected},
		{"the caller's context ended", context.Canceled, nil, LaneBurnOutcomeUnknown},
		{"a spend cut short by the context ending", errors.NewContextCanceledError("cancelled"),
			[]*Spend{{Err: errors.NewContextCanceledError("cancelled")}}, LaneBurnOutcomeUnknown},
		{"a write marked unknown", MarkCreateOutcomeUnknown(errors.NewServiceUnavailableError("timed out")), nil, LaneBurnOutcomeUnknown},
		{"anything else", errors.NewStorageError("disk"), nil, LaneBurnError},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, LaneBurnReason(c.err, c.spends))
		})
	}
}

func TestWithCaller(t *testing.T) {
	for c := CallerUnknown; c < callerCount; c++ {
		var o CreateOptions

		WithCaller(c)(&o)
		require.Equal(t, c, o.Caller)
		require.NotEmpty(t, c.String())
	}

	var o CreateOptions

	WithCaller(callerCount + 3)(&o)
	require.Equal(t, CallerUnknown, o.Caller, "a caller out of range is unknown")
	require.Equal(t, "unknown", Caller(200).String())

	require.Equal(t, "validator", CallerValidator.String())
	require.Equal(t, "subtree_validation", CallerSubtreeValidation.String())
	require.Equal(t, "block_validation", CallerBlockValidation.String())
	require.Equal(t, "block_assembly", CallerBlockAssembly.String())
}

func TestTookLanePosition(t *testing.T) {
	require.True(t, TookLanePosition(0, 5), "no position named, one taken from the source")
	require.False(t, TookLanePosition(5, 5), "the caller named it")
	require.False(t, TookLanePosition(0, 0), "no source")
}

// burnLog captures the info lines a burn writes.
type burnLog struct {
	ulogger.TestLogger
	lines []string
}

func (l *burnLog) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

// Every burn writes one info line naming the transaction, the position and its lane, the store, the
// reason and the caller. A create that wrote its record writes none.
func TestBurnLanePositionLogsOneLine(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("teststore", log)

	tx := bt.NewTx()
	tx.LockTime = 7
	txid := *tx.TxIDChainHash()
	position := lane.Pack(9, 1234)

	positions.BurnCreateLanePosition(tx, &CreateOptions{Caller: CallerBlockValidation}, position, nil, nil)
	require.Empty(t, log.lines, "a create that wrote its record burns nothing")

	positions.BurnCreateLanePosition(tx, &CreateOptions{Caller: CallerBlockValidation}, position,
		errors.NewTxExistsError("exists"), nil)
	require.Len(t, log.lines, 1)

	line := log.lines[0]
	for _, want := range []string{txid.String(), fmt.Sprint(position), "lane 9", "store=teststore",
		"reason=exists", "caller=block_validation"} {
		require.Contains(t, line, want)
	}
}

// A create that names its transaction's ID logs that ID, not the hash of the bytes it was given, as
// a rebuilt transaction's create does.
func TestBurnLanePositionLogsTheNamedTxID(t *testing.T) {
	log := &burnLog{}

	var positions LanePositions

	positions.NameLaneBurns("teststore", log)

	tx := bt.NewTx()
	named := chainhash.HashH([]byte("the id the create names"))
	require.NotEqual(t, named, *tx.TxIDChainHash())

	positions.BurnCreateLanePosition(tx, &CreateOptions{TxID: &named}, lane.Pack(9, 1), errors.NewTxExistsError("exists"), nil)

	require.Len(t, log.lines, 1)
	require.Contains(t, log.lines[0], named.String())
	require.NotContains(t, log.lines[0], tx.TxIDChainHash().String())
}
