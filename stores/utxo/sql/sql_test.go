package sql

import (
	"context"
	"net/url"
	"testing"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/tests"
	"github.com/bitcoin-sv/ubsv/util"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var (
	tx, _        = bt.NewTxFromString("010000000152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	testSpend0   = &utxostore.Spend{
		TxID: tx.TxIDChainHash(),
		Vout: 0,
		Hash: utxoHash0,
	}
	testHash, _  = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	testHash2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	spends       = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: testHash,
	}}
	spends2 = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: testHash2,
	}}
)

func newDB(t *testing.T) (context.Context, *Store) {
	sqliteURL, err := url.Parse("sqlitememory:///utxo")
	require.NoError(t, err)

	// ubsv db client
	db, err := New(sqliteURL)
	require.NoError(t, err)

	return context.Background(), db
}

func TestSQL(t *testing.T) {
	t.Run("memory store", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("memory spend", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("memory reset", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("memory lock time", func(t *testing.T) {
		ctx, db := newDB(t)
		err := db.delete(ctx, tests.Hash)
		require.NoError(t, err)

		tests.LockTime(t, db)
	})
}
