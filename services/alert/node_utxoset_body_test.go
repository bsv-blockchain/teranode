package alert

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestAlertActsOnUnspentOutputsWhoseBodyHasAgedOut drives the alert node against the utxoset
// store after its pruner has dropped the parent's transaction body.
//
// The utxoset store keeps a transaction's serialized bytes for 288 blocks and drops them with
// the pruner, but keeps every unspent output's satoshis and locking script on its coin row for
// as long as the output is unspent. Those two values are all a freeze, an unfreeze or a
// reassignment needs. Reading them through Get(fields.Tx) instead made every alert operation
// on a coin older than 288 blocks fail, although the coin is still there and still spendable
// by its owner, which is exactly the coin a court order is about.
//
// The body is removed by the store's own pruner pass at a height past the retention, the path
// a running node takes, not by a test-only hook.
func TestAlertActsOnUnspentOutputsWhoseBodyHasAgedOut(t *testing.T) {
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

	store, err := utxoset.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(ctx) })

	const (
		createdAt = uint32(1_000)
		// Far enough past createdAt that the body window holding it is below the
		// 288-block retention cutoff.
		prunedAt = createdAt + 1_000
	)

	require.NoError(t, store.SetBlockHeight(createdAt))

	_, err = store.Create(ctx, tx, createdAt)
	require.NoError(t, err)

	// Mined, as the coins an alert targets are. The store keeps the body of a transaction
	// still waiting to be mined past its window, so only a mined one loses it.
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()},
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: createdAt + 1, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, store.SetBlockHeight(prunedAt))

	prunerService, err := store.GetPrunerService()
	require.NoError(t, err)

	_, err = prunerService.Prune(ctx, prunedAt, "")
	require.NoError(t, err)

	// The state under test: the transaction is present and its outputs are live, but the
	// store no longer holds its body.
	parentMeta, err := store.Get(ctx, tx.TxIDChainHash(), fields.Tx)
	require.NoError(t, err)
	require.Nil(t, parentMeta.Tx, "the pruner must have dropped the body for this test to mean anything")

	resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 0})
	require.NoError(t, err)
	require.Equal(t, int(utxo.Status_OK), resp.Status, "the output itself is still unspent")

	node := NewNodeConfig(ulogger.TestLogger{}, nil, store, nil, nil, nil, tSettings)

	t.Run("freeze", func(t *testing.T) {
		response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
			TxOut:           models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 0},
			EnforceAtHeight: []models.Enforce{{Start: int(createdAt), Stop: 999999999}},
		}})
		require.NoError(t, err)
		require.Empty(t, response.NotProcessed)

		// End state: the owner's spend of the output is refused as frozen.
		child := bt.NewTx()
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash:      tx.TxIDChainHash(),
			Vout:          0,
			LockingScript: tx.Outputs[0].LockingScript,
			Satoshis:      tx.Outputs[0].Satoshis,
		}))
		child.AddOutput(&bt.Output{Satoshis: tx.Outputs[0].Satoshis - 1_000, LockingScript: tx.Outputs[0].LockingScript})

		_, spends, err := store.SpendAndCreate(ctx, child, prunedAt+1, utxo.WithSpendOnly())
		require.Error(t, err)
		require.Len(t, spends, 1)
		require.True(t, errors.Is(spends[0].Err, errors.ErrFrozen), "got %v", spends[0].Err)
	})

	t.Run("unfreeze", func(t *testing.T) {
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 2}}, tSettings))

		response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
			TxOut: models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 2},
			// Stop below the current height is the unfreeze branch.
			EnforceAtHeight: []models.Enforce{{Start: 100, Stop: 100}},
		}})
		require.NoError(t, err)
		require.Empty(t, response.NotProcessed)

		resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: 2})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_OK), resp.Status)
	})

	t.Run("reassign a frozen output", func(t *testing.T) {
		const vout = 1

		oldHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[vout], vout)
		require.NoError(t, err)

		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: vout, UTXOHash: oldHash}}, tSettings))

		privateKey, err := bec.NewPrivateKey()
		require.NoError(t, err)

		lockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().Compressed())
		require.NoError(t, err)

		confiscationTransaction := bt.Tx{}
		require.NoError(t, confiscationTransaction.FromUTXOs([]*bt.UTXO{{
			TxIDHash:       tx.TxIDChainHash(),
			Vout:           vout,
			LockingScript:  lockingScript,
			Satoshis:       tx.Outputs[vout].Satoshis,
			SequenceNumber: bt.DefaultSequenceNumber,
		}}...))
		require.NoError(t, confiscationTransaction.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", tx.Outputs[vout].Satoshis))
		require.NoError(t, confiscationTransaction.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

		response, err := node.AddToConfiscationTransactionWhitelist(ctx, []models.ConfiscationTransactionDetails{{
			ConfiscationTransaction: models.ConfiscationTransaction{
				EnforceAtHeight: int64(prunedAt + 1),
				Hex:             confiscationTransaction.String(),
			},
		}})
		require.NoError(t, err)
		require.Empty(t, response.NotProcessed)

		// End state, as the existing reassign tests check it: the old owner's hash no longer
		// answers, and the new one finds the output held by the reassignment delay.
		_, err = store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: vout, UTXOHash: oldHash})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrUtxoHashMismatch), "got %v", err)

		newHash, err := util.UTXOHashFromInput(confiscationTransaction.Inputs[0])
		require.NoError(t, err)

		resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: vout, UTXOHash: newHash})
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_IMMATURE), resp.Status)
	})

	t.Run("out-of-range vout", func(t *testing.T) {
		response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
			TxOut:           models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 99},
			EnforceAtHeight: []models.Enforce{{Start: int(createdAt), Stop: 999999999}},
		}})
		require.NoError(t, err)
		require.Len(t, response.NotProcessed, 1)
		require.Contains(t, response.NotProcessed[0].Reason, "output 99 not found")
	})

	t.Run("unknown parent", func(t *testing.T) {
		unknown := "00000000000000000000000000000000000000000000000000000000000000aa"

		response, err := node.AddToConsensusBlacklist(ctx, []models.Fund{{
			TxOut:           models.TxOut{TxId: unknown, Vout: 0},
			EnforceAtHeight: []models.Enforce{{Start: int(createdAt), Stop: 999999999}},
		}})
		require.NoError(t, err)
		require.Len(t, response.NotProcessed, 1)
		require.Contains(t, response.NotProcessed[0].Reason, "not found")
	})
}
