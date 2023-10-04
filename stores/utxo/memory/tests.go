package memory

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	testHash1, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	testHash2, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
)

func testStore(t *testing.T, db utxostore.Interface) {
	ctx := context.Background()

	resp, err := db.Store(ctx, testHash1, 0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Get(ctx, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Store(context.Background(), testHash1, 0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(context.Background(), testHash1, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Store(context.Background(), testHash1, 0)
	require.NoError(t, err)
	// this value should be spent, but we delete spends, so it will now be ok (stored again)
	require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
}

func testSpend(t *testing.T, db utxostore.Interface) {
	ctx := context.Background()

	resp, err := db.Store(ctx, testHash1, 0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(ctx, testHash1, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
	require.Equal(t, *testHash1, *resp.SpendingTxID)

	// try to spend with different txid
	resp, err = db.Spend(context.Background(), testHash1, testHash2)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
}

func testRestore(t *testing.T, db utxostore.Interface) {
	ctx := context.Background()

	resp, err := db.Store(ctx, testHash1, 0)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(ctx, testHash1, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	// try to reset the utxo
	resp, err = db.Reset(ctx, testHash1)
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
	require.Nil(t, resp.SpendingTxID)
}

func testLockTime(t *testing.T, db utxostore.Interface) {
	ctx := context.Background()

	resp, err := db.Store(ctx, testHash1, 1000)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(ctx, testHash1, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)
	require.Equal(t, uint32(1000), resp.LockTime)

	_ = db.SetBlockHeight(1000)

	resp, err = db.Spend(ctx, testHash1, testHash1)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
	require.Equal(t, testHash1, resp.SpendingTxID)
}

func testSanity(t *testing.T, db utxostore.Interface) {
	util.SkipLongTests(t)
	ctx := context.Background()

	var resp *utxostore.UTXOResponse
	var err error
	bs := make([]byte, 32)
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)

		resp, err = db.Store(ctx, &ii, 0)
		if resp.Status != int(utxostore_api.Status_OK) {
			s := ii.String()
			_ = s
		}
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(ctx, &ii, &iii)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
		require.Equal(t, iii, *resp.SpendingTxID)
	}

	for i := uint64(0); i < 1_000_000; i++ {
		// check values
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)

		resp, err = db.Get(ctx, &ii)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
		require.Equal(t, iii, *resp.SpendingTxID)
	}
}

func benchmark(b *testing.B, db utxostore.Interface) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		resp, err := db.Store(ctx, testHash1, 0)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Status != int(utxostore_api.Status_OK) {
			b.Fatal("unexpected status")
		}

		resp, err = db.Spend(ctx, testHash1, testHash2)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Status != int(utxostore_api.Status_OK) {
			b.Fatal("unexpected status")
		}

		_, err = db.Reset(ctx, testHash1)
		if err != nil {
			b.Fatal(err)
		}
	}
}
