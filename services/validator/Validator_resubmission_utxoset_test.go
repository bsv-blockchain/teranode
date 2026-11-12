package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidate_ResubmissionAfterJournalDropOnUtxoset runs the case knownTxResubmission exists
// for against a real utxoset store rather than a mock: a mined transaction is submitted again
// after the spend-journal leaf that recorded its spends has been dropped. Its coins are gone
// and nothing names who took them, so the spend comes back ErrSpent with no spender, and only
// a lookup under the transaction's own txid can tell that it is the same transaction.
//
// TestValidate_ResubmissionIsRecognisedByItsOwnTxid pins the branch logic case by case; this
// test pins that the store really produces the shape the branch keys on, and that a different
// transaction spending the same dropped coin is still rejected.
func TestValidate_ResubmissionAfterJournalDropOnUtxoset(t *testing.T) {
	ctx := context.Background()

	dsn, cleanup, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		test.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	// No block-assembly client is wired; the hand-off is not what this test is about.
	tSettings.BlockAssembly.Disabled = true

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(ctx) })

	const (
		height = uint32(1_000)
		// Block-context validation above CSV activation takes the candidate parent's median
		// time from the caller. The transaction has no lock time, so any value will do.
		candidateMTP = uint32(1_000_000_000)
	)

	require.NoError(t, store.SetBlockHeight(height))

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	// The parent is written straight to the store; only the child goes through validation.
	// It spends a coinbase the store never sees, so it is not itself a coinbase and its
	// output carries no maturity.
	grandparent := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	parent := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(grandparent, 0),
		transactions.WithP2PKHOutputs(2, 20e8, publicKey),
	)

	_, err = store.Create(ctx, parent, height-10)
	require.NoError(t, err)

	// Mined, because block-context validation refuses an unconfirmed input.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 3, BlockHeight: height - 10, OnLongestChain: true})
	require.NoError(t, err)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0),
		transactions.WithP2PKHOutputs(1, 1000),
		transactions.WithChangeOutput(),
	)

	vi, err := New(ctx, ulogger.TestLogger{}, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	v := vi.(*Validator)

	_, err = v.Validate(ctx, tx, height, WithSkipPolicyChecks(true), WithCandidateParentMedianTime(candidateMTP))
	require.NoError(t, err, "the first submission spends the parent's output")

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 7, BlockHeight: height + 1, OnLongestChain: true})
	require.NoError(t, err)

	// Move far enough past the spend that its journal leaf is below the cutoff, then drop it.
	dropBelow := (height/utxoset.SpendJournalPartitionBlocks + 2) * utxoset.SpendJournalPartitionBlocks
	require.NoError(t, store.SetBlockHeight(dropBelow))

	dropped, err := store.DropSpendJournalPartitionsBelow(ctx, dropBelow)
	require.NoError(t, err)
	require.Positive(t, dropped, "the leaf holding the spend must actually be gone for this test to mean anything")

	got, err := v.Validate(ctx, tx, dropBelow, WithSkipPolicyChecks(true), WithCandidateParentMedianTime(candidateMTP))
	require.NoError(t, err, "a mined transaction submitted again after its journal leaf dropped is the same transaction, not a double spend")
	require.NotNil(t, got)
	require.Equal(t, []uint32{7}, got.BlockIDs)

	// A different transaction asking for the same coin is a double spend. Its own txid is not
	// in the store, so the lookup that recognised the resubmission does not excuse it.
	doubleSpend := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0),
		transactions.WithP2PKHOutputs(1, 2000),
		transactions.WithChangeOutput(),
	)
	require.NotEqual(t, *tx.TxIDChainHash(), *doubleSpend.TxIDChainHash())

	got, err = v.Validate(ctx, doubleSpend, dropBelow, WithSkipPolicyChecks(true), WithCandidateParentMedianTime(candidateMTP))
	require.Error(t, err)
	require.Nil(t, got)
	require.True(t, errors.Is(err, errors.ErrSpent), "expected the double spend to fail as spent, got: %v", err)
}
