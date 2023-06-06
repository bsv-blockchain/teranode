package memory

import (
	"context"
	"os"
	"testing"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	hash, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	// hash2, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
)

func testStore(t *testing.T, db txstatus.Store) {
	ctx := context.Background()

	err := db.Set(ctx, hash, 100, nil)
	require.NoError(t, err)

	resp, err := db.Get(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, txstatus.Unconfirmed, resp.Status)

	err = db.Set(ctx, hash, 100, nil)
	require.Error(t, err, txstatus.ErrAlreadyExists)
}

func testSanity(t *testing.T, db txstatus.Store) {
	skipLongTests(t)
}

func benchmark(b *testing.B, db txstatus.Store) {
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		err := db.Set(ctx, hash, 100, nil)
		if err != nil {
			b.Fatal(err)
		}

		status, err := db.Get(ctx, hash)
		if err != nil {
			b.Fatal(err)
		}
		if status.Status != txstatus.Unconfirmed {
			b.Fatal(status)
		}

		_ = db.Delete(ctx, hash)
	}
}

func skipLongTests(t *testing.T) {
	if os.Getenv("LONG_TESTS") == "" {
		t.Skip("Skipping long running tests. Set LONG_TESTS=1 to run them.")
	}
}
