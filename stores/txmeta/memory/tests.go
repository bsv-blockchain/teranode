package memory

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	hash, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	// hash2, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
)

func testStore(t *testing.T, db txmeta.Store) {
	ctx := context.Background()

	err := db.Create(ctx, hash, 100, nil, nil)
	require.NoError(t, err)

	resp, err := db.Get(ctx, hash)
	require.NoError(t, err)
	require.Equal(t, txmeta.Validated, resp.Status)

	err = db.Create(ctx, hash, 100, nil, nil)
	require.Error(t, err, txmeta.ErrAlreadyExists)
}

func testSanity(t *testing.T, db txmeta.Store) {
	skipLongTests(t)
}

func benchmark(b *testing.B, db txmeta.Store) {
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := make([]byte, 32)
			_, err := rand.Read(buf)
			require.NoError(b, err)

			bHash, _ := chainhash.NewHash(buf)

			err = db.Create(ctx, bHash, 100, nil, nil)
			if err != nil {
				b.Fatal(err)
			}

			status, err := db.Get(ctx, bHash)
			if err != nil {
				b.Fatal(err)
			}
			if status.Status != txmeta.Validated {
				b.Fatal(status)
			}
		}
	})
}

func skipLongTests(t *testing.T) {
	if os.Getenv("LONG_TESTS") == "" {
		t.Skip("Skipping long running tests. Create LONG_TESTS=1 to run them.")
	}
}
