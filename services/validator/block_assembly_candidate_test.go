package validator

import (
	"context"
	"net/url"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// opTrueSpend returns a tx spending output 0 of parent, an OP_TRUE output, with the given
// version, lock time and input sequence. No signature, so any of them can be set freely.
func opTrueSpend(t *testing.T, parent *bt.Tx, version, lockTime, sequence uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Version = version
	tx.LockTime = lockTime

	input := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{}), SequenceNumber: sequence}
	require.NoError(t, input.PreviousTxIDAdd(parent.TxIDChainHash()))
	input.PreviousTxSatoshis = parent.Outputs[0].Satoshis
	input.PreviousTxScript = parent.Outputs[0].LockingScript
	tx.Inputs = append(tx.Inputs, input)

	tx.AddOutput(&bt.Output{Satoshis: parent.Outputs[0].Satoshis - 100, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})

	return tx
}

// TestValidate_BlockAssemblyCandidateGatesTheHandoff pins the hand-off of a side block's
// transaction to block assembly. The transaction is valid in its block, but block assembly
// mines it at the candidate (tip+1, MTP of the tip) without re-checking it, so it is handed
// over only if it is final there and carries no active BIP68 relative lock. Otherwise it is
// accepted without a template entry, as with WithAddTXToBlockAssembly(false).
func TestValidate_BlockAssemblyCandidateGatesTheHandoff(t *testing.T) {
	const (
		blockHeight = 3
		lockTime    = 1_700_000_000 // a time lock
		blockTime   = lockTime + 100
		sequence    = 0xfffffffe // lock time enforced, BIP68 disabled below version 2
	)

	tests := []struct {
		name      string
		version   uint32
		sequence  uint32
		candidate *BlockAssemblyCandidate
		handoff   bool
	}{
		{"no candidate: the block is the tip extension", 1, sequence, nil, true},
		{"final at the candidate", 1, sequence, &BlockAssemblyCandidate{Height: blockHeight + 1, MedianTimePast: lockTime + 1}, true},
		{"not final at the candidate", 1, sequence, &BlockAssemblyCandidate{Height: blockHeight + 1, MedianTimePast: lockTime - 1}, false},
		{"active relative lock past CSV", 2, 0, &BlockAssemblyCandidate{Height: 1000, MedianTimePast: lockTime + 1}, false},
		{"relative lock disabled past CSV", 2, 0xffffffff, &BlockAssemblyCandidate{Height: 1000, MedianTimePast: lockTime + 1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.Disabled = false

			utxoStoreURL, err := url.Parse("sqlitememory:///test")
			require.NoError(t, err)

			utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
			require.NoError(t, err)
			require.NoError(t, utxoStore.SetBlockHeight(blockHeight-1))
			require.NoError(t, utxoStore.SetMedianBlockTime(blockTime))

			parent := bt.NewTx()
			parent.AddOutput(&bt.Output{Satoshis: 10_000, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})
			_, err = utxoStore.Create(ctx, parent, 1, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 1, BlockHeight: 1, SubtreeIdx: 0}))
			require.NoError(t, err)

			tx := opTrueSpend(t, parent, tt.version, lockTime, tt.sequence)

			blockAsmMock := blockassembly.NewMock()
			blockAsmMock.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil).Maybe()

			v := &Validator{
				logger:         ulogger.TestLogger{},
				utxoStore:      utxoStore,
				blockAssembler: blockAsmMock,
				settings:       tSettings,
				txValidator:    NewTxValidator(ulogger.TestLogger{}, tSettings),
				stats:          gocore.NewStat("validator"),
			}

			opts := ProcessOptions(
				WithSkipPolicyChecks(true),
				WithInBlock(true),
				WithCandidateBlockTime(blockTime),
				WithBlockAssemblyCandidate(tt.candidate),
			)

			_, err = v.ValidateWithOptions(ctx, tx, blockHeight, opts)
			require.NoError(t, err)

			if tt.handoff {
				blockAsmMock.AssertCalled(t, "Store", mock.Anything, tx.TxIDChainHash(), mock.Anything, mock.Anything, mock.Anything)
			} else {
				blockAsmMock.AssertNotCalled(t, "Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
			}

			metaData := &meta.Data{}
			require.NoError(t, utxoStore.GetMeta(ctx, tx.TxIDChainHash(), metaData))
			require.False(t, metaData.Locked, "an accepted block transaction is never left locked")
		})
	}
}

// TestOptionsFromValidateRequest_BlockAssemblyCandidateRoundTrip pins the Client → Server
// gRPC propagation of the block assembly candidate.
func TestOptionsFromValidateRequest_BlockAssemblyCandidateRoundTrip(t *testing.T) {
	src := &Options{BlockAssemblyCandidate: &BlockAssemblyCandidate{Height: 943_900, MedianTimePast: 1_700_000_000}}

	wire, err := proto.Marshal(buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 943_890, src))
	require.NoError(t, err)

	req := &validator_api.ValidateTransactionRequest{}
	require.NoError(t, proto.Unmarshal(wire, req))

	got, err := optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.Equal(t, src.BlockAssemblyCandidate, got.BlockAssemblyCandidate)

	req = buildValidateTxRequest(newTinyTx(t).SerializeBytes(), 943_890, &Options{})
	got, err = optionsFromValidateRequest(req)
	require.NoError(t, err)
	require.Nil(t, got.BlockAssemblyCandidate)
}
