package validator

import (
	"context"
	"net/url"
	"slices"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// The UTXO commitment preimage is
//
//	previousTxid || VarInt(vout) || lockingScript || VarInt(satoshis)
//
// (util.UTXOHashInto). The locking script carries no length prefix, so the
// boundary between the script and the satoshi VarInt is not pinned: a shorter
// script paired with a larger satoshi value can reproduce the same bytes.
//
// These fixtures are two such pairs. Each is a (real script, real satoshis)
// output whose commitment is also produced by (forged script, forged satoshis),
// where the forged script is a spendable OP_TRUE and the forged value is
// inflated.
type ambiguityFixture struct {
	name string
	// as stored in the UTXO store — the genuine, unspendable output
	realScript []byte
	realSats   uint64
	// as claimed by an attacker in an extended transaction
	forgedScript []byte
	forgedSats   uint64
}

var ambiguityFixtures = []ambiguityFixture{
	{
		// 4-byte (0xfe) satoshi VarInt form.
		//   real:   51 fe ff ff ff || fc          (252 sats)
		//   forged: 51             || fe ff ff fc (4,244,635,647 sats)
		name:         "fe_varint_42_BSV",
		realScript:   []byte{0x51, 0xfe, 0xff, 0xff, 0xff},
		realSats:     252,
		forgedScript: []byte{0x51},
		forgedSats:   4244635647,
	},
	{
		// 8-byte (0xff) satoshi VarInt form. One zero-value output is enough to
		// claim the entire 21,000,000 BSV money range; no batching required.
		name:         "ff_varint_full_money_range",
		realScript:   []byte{0x51, 0xff, 0x00, 0x40, 0x07, 0x5a, 0xf0, 0x75, 0x07},
		realSats:     0,
		forgedScript: []byte{0x51},
		forgedSats:   2100000000000000,
	},
}

// newAmbiguityValidator builds a validator over a real sqlitememory UTXO store
// holding a single parent transaction whose output 0 is the fixture's genuine
// (script, satoshis) pair.
func newAmbiguityValidator(t *testing.T, dbName string, f ambiguityFixture) (*Validator, *bt.Tx, context.Context) {
	t.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, parseErr := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, parseErr)

	store, storeErr := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, storeErr)
	require.NoError(t, store.SetBlockHeight(500))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	fundingScript, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// The parent is itself extended so Create can compute its fee; this release
	// branch has no WithSkipExtendedInputs.
	parent := bt.NewTx()
	parentInput := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x00}),
		PreviousTxScript:   fundingScript,
		PreviousTxSatoshis: 100_000,
	}
	require.NoError(t, parentInput.PreviousTxIDAdd(&chainhash.Hash{1}))
	parent.Inputs = append(parent.Inputs, parentInput)

	realScript := bscript.Script(f.realScript)
	parent.Outputs = append(parent.Outputs, &bt.Output{
		Satoshis:      f.realSats,
		LockingScript: &realScript,
	})

	_, err = store.Create(ctx, parent, 499)
	require.NoError(t, err)

	v := &Validator{
		logger:      logger,
		utxoStore:   store,
		settings:    tSettings,
		txValidator: NewTxValidator(logger, tSettings),
		stats:       gocore.NewStat("validator"),
	}

	return v, parent, ctx
}

// spendOf builds an extended transaction spending parent:0 while claiming the
// given previous-output script and value.
func spendOf(t *testing.T, parent *bt.Tx, claimScript []byte, claimSats, outSats uint64) *bt.Tx {
	t.Helper()

	script := bscript.Script(claimScript)
	tx := bt.NewTx()
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     0xffffffff,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
		PreviousTxScript:   &script,
		PreviousTxSatoshis: claimSats,
	}
	require.NoError(t, input.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = append(tx.Inputs, input)

	payTo, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: outSats, LockingScript: payTo})

	return tx
}

// TestValidate_RejectsForgedPreviousOutputInExtendedTx is the GHSA-v76m-6vc7-g7c7
// regression test for the validator ingress leg (public /tx, /txs and gRPC all
// reach Validate with the submitter's extended fields intact).
//
// Fails before the fix with err == nil: the validator skipped re-extension for
// an already-extended transaction, so BDK executed the attacker's OP_TRUE and
// value conservation used the attacker's inflated satoshi claim, while the
// store's utxoHash comparison passed on the collision.
func TestValidate_RejectsForgedPreviousOutputInExtendedTx(t *testing.T) {
	for _, f := range ambiguityFixtures {
		t.Run(f.name, func(t *testing.T) {
			v, parent, ctx := newAmbiguityValidator(t, "ambiguity_forged_"+f.name, f)

			forged := spendOf(t, parent, f.forgedScript, f.forgedSats, f.forgedSats-1)

			// AddTXToBlockAssembly is off only because this bare Validator has no
			// block assembler wired; every security-relevant check still runs.
			_, err := v.ValidateWithOptions(ctx, forged, 500, &Options{AddTXToBlockAssembly: false})
			require.Error(t, err, "forged previous-output claim must be rejected")
			require.True(t, errors.Is(err, errors.ErrTxInvalid),
				"must be rejected as an invalid transaction, got: %v", err)
			require.Contains(t, err.Error(), "bad-txns-in-belowout",
				"must fail value conservation against the real parent value")
		})
	}
}

// TestValidate_RejectsGenuineUnspendableOutput pins the control: the honest
// spend of the same UTXO, declaring its real script and value, must stay
// rejected. The fixtures' real scripts contain byte 0xfe / 0xff in opcode
// position and are genuinely unspendable, which is what makes the forged
// alternative worth constructing.
func TestValidate_RejectsGenuineUnspendableOutput(t *testing.T) {
	for _, f := range ambiguityFixtures {
		t.Run(f.name, func(t *testing.T) {
			v, parent, ctx := newAmbiguityValidator(t, "ambiguity_honest_"+f.name, f)
			// The zero-value fixture cannot pay a fee. Disable the fee requirement
			// explicitly so CI policy settings cannot mask the script failure.
			v.settings.Policy.MinMiningTxFee = 0
			v.txValidator = NewTxValidator(v.logger, v.settings)

			honest := spendOf(t, parent, f.realScript, f.realSats, 0)

			_, err := v.ValidateWithOptions(ctx, honest, 500, &Options{AddTXToBlockAssembly: false})
			require.Error(t, err, "genuine output is unspendable and must stay rejected")
			require.True(t, errors.Is(err, errors.ErrTxInvalid),
				"must be rejected as an invalid transaction, got: %v", err)
			require.Contains(t, err.Error(), "Opcode missing or not understood",
				"the genuine script is unspendable because of its invalid opcode")
		})
	}
}

// fieldSpyStore wraps a real utxo.Store and records the field projection of
// every Get, so a test can assert what the validator actually asks the store for.
type fieldSpyStore struct {
	utxostore.Store

	mu      sync.Mutex
	getters [][]fields.FieldName
}

func (s *fieldSpyStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.mu.Lock()
	s.getters = append(s.getters, append([]fields.FieldName(nil), f...))
	s.mu.Unlock()

	return s.Store.Get(ctx, hash, f...)
}

func (s *fieldSpyStore) requested(want fields.FieldName) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range s.getters {
		if slices.Contains(f, want) {
			return true
		}
	}

	return false
}

// TestValidate_ReExtendUsesOutputsProjection pins the projection the re-extension
// path reads with.
//
// Inline Aerospike parents omit the inputs bin with fields.Outputs. External
// parents still require the full body, and SQL adds an outputs query. The test
// pins the requested projection, not a claim that extension adds no reads.
func TestValidate_ReExtendUsesOutputsProjection(t *testing.T) {
	f := ambiguityFixtures[0]
	v, parent, ctx := newAmbiguityValidator(t, "ambiguity_projection", f)

	spy := &fieldSpyStore{Store: v.utxoStore}
	v.utxoStore = spy

	forged := spendOf(t, parent, f.forgedScript, f.forgedSats, f.forgedSats-1)
	_, err := v.ValidateWithOptions(ctx, forged, 500, &Options{AddTXToBlockAssembly: false})
	require.Error(t, err, "forged claim must still be rejected")

	require.True(t, spy.requested(fields.Outputs),
		"re-extension must read the parent with the narrow outputs projection")
	require.False(t, spy.requested(fields.Tx),
		"re-extension must not pull the parent's inputs")
}
