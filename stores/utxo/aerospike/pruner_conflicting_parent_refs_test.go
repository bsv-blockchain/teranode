package aerospike_test

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestPrunerClearsParentRefsOfReapedConflictingLoser verifies the pruner, in defensive mode only,
// removes every reference a surviving parent holds to a conflicting loser before it deletes the
// loser: the parent output spend and the parent conflictingChildren entry.
func TestPrunerClearsParentRefsOfReapedConflictingLoser(t *testing.T) {
	for _, defensive := range []bool{false, true} {
		t.Run(fmt.Sprintf("defensive=%v", defensive), func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			s := test.CreateBaseTestSettings(t)
			s.UtxoStore.DisableDAHCleaner = false
			s.Pruner.UTXODefensiveEnabled = defensive

			client, store, ctx, cleanup := initAerospike(t, s, logger)
			t.Cleanup(cleanup)

			require.NoError(t, store.SetBlockHeight(1000))

			retention := s.GetUtxoStoreBlockHeightRetention()
			require.NotZero(t, retention)

			astore.ResetPrunerServiceForTests()
			t.Cleanup(astore.ResetPrunerServiceForTests)
			require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
			require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

			svc, err := store.GetPrunerService()
			require.NoError(t, err)

			prunerSvc, ok := svc.(*apruner.Service)
			require.True(t, ok)

			parent := bt.NewTx()
			require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
			_, err = store.Create(ctx, parent, 1000)
			require.NoError(t, err)

			loser := bt.NewTx()
			require.NoError(t, loser.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
			require.NoError(t, loser.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
			_, _, err = store.SpendAndCreate(ctx, loser, 1000)
			require.NoError(t, err)

			loserHash := loser.TxIDChainHash()

			_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*loserHash}, true)
			require.NoError(t, err)

			utxoHash, err := util.UTXOHashFromInput(loser.Inputs[0])
			require.NoError(t, err)

			parentSpend := &utxo.Spend{TxID: parent.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}

			before, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)
			require.NotNil(t, before.SpendingData, "precondition: the parent output is spent by the loser")

			parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash()[:])
			require.NoError(t, err)

			conflictingChildren := func() int {
				rec, gerr := client.Get(nil, parentKey, fields.ConflictingChildren.String())
				require.NoError(t, gerr)

				list, _ := rec.Bins[fields.ConflictingChildren.String()].([]interface{})

				return len(list)
			}

			require.Equal(t, 1, conflictingChildren(), "precondition: the parent lists the loser as a conflicting child")

			n, err := prunerSvc.PruneWithPartitions(ctx, uint32(1000+int(retention)), "conflicting", 1)
			require.NoError(t, err)
			require.Equal(t, int64(1), n, "the conflicting loser must be reaped")

			_, err = store.Get(ctx, loserHash)
			require.Error(t, err)

			after, err := store.GetSpend(ctx, parentSpend)
			require.NoError(t, err)

			if defensive {
				require.Nil(t, after.SpendingData, "a surviving parent must not name the reaped loser as its spender")
				require.Equal(t, 0, conflictingChildren(), "a surviving parent must not list the reaped loser as a conflicting child")
			} else {
				require.NotNil(t, after.SpendingData, "default mode keeps the existing behaviour")
			}
		})
	}
}
