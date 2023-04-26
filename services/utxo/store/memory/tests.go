package memory

import (
	"context"
	"encoding/binary"
	"os"
	"testing"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	hash, _  = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	hash2, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
)

func testStore(t *testing.T, db store.UTXOStore) {
	ctx := context.Background()

	resp, err := db.Store(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Get(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Store(context.Background(), hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(context.Background(), hash, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Store(context.Background(), hash)
	require.NoError(t, err)
	// this value should be spent, but we delete spends, so it will now be ok (stored again)
	require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
}

func testSpend(t *testing.T, db store.UTXOStore) {
	ctx := context.Background()

	resp, err := db.Store(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(ctx, hash, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
	require.Equal(t, *hash, *resp.SpendingTxID)

	// try to spend with different txid
	resp, err = db.Spend(context.Background(), hash, hash2)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
}

func testRestore(t *testing.T, db store.UTXOStore) {
	ctx := context.Background()

	resp, err := db.Store(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

	resp, err = db.Spend(ctx, hash, hash)
	require.NoError(t, err)

	// try to reset the utxo
	resp, err = db.Reset(ctx, hash)
	require.NoError(t, err)

	require.NoError(t, err)
	require.Equal(t, int(utxostore_api.Status_OK), resp.Status)
	require.Nil(t, resp.SpendingTxID)
}

func testSanity(t *testing.T, db store.UTXOStore) {
	//skipLongTests(t)
	ctx := context.Background()

	var resp *store.UTXOResponse
	var err error
	bs := make([]byte, 32)
	for i := uint64(0); i < 1_000_000; i++ {
		binary.LittleEndian.PutUint64(bs, i)
		ii := chainhash.HashH(bs)
		binary.LittleEndian.PutUint64(bs, i+2_000_000)
		iii := chainhash.HashH(bs)

		resp, err = db.Store(ctx, &ii)
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

func benchmark(b *testing.B, db store.UTXOStore) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		resp, err := db.Store(ctx, hash)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Status != int(utxostore_api.Status_OK) {
			b.Fatal("unexpected status")
		}

		resp, err = db.Spend(ctx, hash, hash2)
		if err != nil {
			b.Fatal(err)
		}
		if resp.Status != int(utxostore_api.Status_OK) {
			b.Fatal("unexpected status")
		}

		resp, err = db.Reset(ctx, hash)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func skipLongTests(t *testing.T) {
	if os.Getenv("LONG_TESTS") == "" {
		t.Skip("Skipping long running tests. Set LONG_TESTS=1 to run them.")
	}
}
