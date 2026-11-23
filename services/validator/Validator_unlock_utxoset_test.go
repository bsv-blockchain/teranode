package validator

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestValidate_TwoPhaseCommitUnlocksOnUtxoset runs the validator's two-phase commit against a
// real utxoset store: the transaction is created locked, delivered to block assembly, and then
// unlocked so its children can spend it.
//
// The unlock is gated on the record the create returned saying Locked. The utxoset store wrote
// the Locked bit to the row but returned Locked=false, so the validator skipped the unlock, the
// row stayed locked after a successful delivery, and the next transaction spending it was
// refused with ErrTxLocked. The test checks the end state rather than the returned field: the
// stored record must be unlocked and a child spending it must validate.
func TestValidate_TwoPhaseCommitUnlocksOnUtxoset(t *testing.T) {
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
	// Block assembly on, as in production: that is what makes the validator create locked and
	// unlock after delivery.
	tSettings.BlockAssembly.Disabled = false

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(ctx) })

	const height = uint32(1_000)

	require.NoError(t, store.SetBlockHeight(height))
	//nolint:gosec // a current unix time fits uint32
	require.NoError(t, store.SetMedianBlockTime(uint32(time.Now().Unix())))

	privateKey, publicKey := bec.PrivateKeyFromBytes([]byte("THIS_IS_A_DETERMINISTIC_PRIVATE_KEY"))

	grandparent := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 50e8, publicKey),
	)
	parent := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(grandparent, 0),
		transactions.WithP2PKHOutputs(2, 20e8, publicKey),
	)

	// The parent is written straight to the store and mined; only tx and its child go through
	// the validator.
	_, err = store.Create(ctx, parent, height-10)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 3, BlockHeight: height - 10, OnLongestChain: true})
	require.NoError(t, err)

	blockAssemblyClient := &blockassembly.Mock{}
	blockAssemblyClient.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

	vi, err := New(ctx, ulogger.TestLogger{}, tSettings, store,
		kafka.NewKafkaAsyncProducerMock(), kafka.NewKafkaAsyncProducerMock(), nil,
		blockAssemblyClient, nil)
	require.NoError(t, err)

	v := vi.(*Validator)

	tx := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(parent, 0),
		transactions.WithP2PKHOutputs(1, 10e8, publicKey),
		transactions.WithChangeOutput(),
	)

	_, err = v.Validate(ctx, tx, height)
	require.NoError(t, err)

	blockAssemblyClient.AssertNumberOfCalls(t, "Store", 1)

	stored, err := store.Get(ctx, tx.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.False(t, stored.Locked, "a transaction delivered to block assembly must be unlocked by the two-phase commit")

	// The child spends tx's first output while tx is still unmined, which is the case the
	// unlock exists for.
	child := transactions.Create(t,
		transactions.WithPrivateKey(privateKey),
		transactions.WithInput(tx, 0),
		transactions.WithP2PKHOutputs(1, 1e8, publicKey),
		transactions.WithChangeOutput(),
	)

	_, err = v.Validate(ctx, child, height)
	require.NoError(t, err, "a child of a delivered and unlocked transaction must validate")

	blockAssemblyClient.AssertNumberOfCalls(t, "Store", 2)
}
