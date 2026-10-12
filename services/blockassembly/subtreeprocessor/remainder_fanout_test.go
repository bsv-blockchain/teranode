package subtreeprocessor

import (
	"context"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func newRemainderFanOutProcessor(t *testing.T) *SubtreeProcessor {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.TxMapDirs = nil
	tSettings.BlockAssembly.SubtreeMmapDir = ""

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(t.Context(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	subtreeStore, _ := null.New(ulogger.TestLogger{})

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, subtreeStore, &blockchain.Mock{}, utxoStore, make(chan NewSubtreeRequest, 16))
	require.NoError(t, err)

	return stp
}

// TestProcessRemainderTxHashes_BoundedFanOut pins that the lookup phase of the
// leftover pass runs on a bounded number of goroutines. It used to start
// ProcessRemainderTxHashesConcurrency subtrees at once with up to
// runtime.NumCPU() workers each - tens of thousands of CPU-bound goroutines on
// a large block - and while they ran, the gRPC goroutines that ingest
// transactions waited hundreds of milliseconds for a P (seen on the scaling
// cluster as validator-to-block-assembly latency spikes during every follower
// moveForwardBlock).
func TestProcessRemainderTxHashes_BoundedFanOut(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(4))

	stp := newRemainderFanOutProcessor(t)

	const (
		subtrees    = 128
		perSubtree  = 16 << 10
		totalHashes = subtrees * perSubtree
	)

	// Every node is in the block, so the pass finds no remainder and only the
	// lookup phase does any work.
	transactionMap := NewSplitSwissMap(64, totalHashes)
	chained := make([]*subtreepkg.Subtree, 0, subtrees)

	for s := 0; s < subtrees; s++ {
		st, err := subtreepkg.NewTreeByLeafCount(perSubtree)
		require.NoError(t, err)

		for i := 0; i < perSubtree; i++ {
			h := chainhash.HashH([]byte{byte(s), byte(s >> 8), byte(i), byte(i >> 8), 'f'})
			require.NoError(t, st.AddNode(h, 1, 1))
			require.NoError(t, transactionMap.Put(h))
		}

		chained = append(chained, st)
	}

	transactionMap.Freeze()

	baseline := runtime.NumGoroutine()

	var (
		peak atomic.Int64
		stop atomic.Bool
		done = make(chan struct{})
	)

	go func() {
		defer close(done)

		for !stop.Load() {
			if n := int64(runtime.NumGoroutine()); n > peak.Load() {
				peak.Store(n)
			}

			time.Sleep(20 * time.Microsecond)
		}
	}()

	require.NoError(t, stp.processRemainderTxHashes(context.Background(), chained, transactionMap, nil, stp.currentTxMap, true))

	stop.Store(true)
	<-done

	// The sampler itself, plus the bounded workers, plus slack: NumGoroutine
	// counts the whole test binary, and goroutines left winding down by earlier
	// tests in the package (about a thousand on CI) come and go while this
	// runs. The unbounded fan-out this guards against started subtrees x
	// NumCPU goroutines (512 on a 4-core runner), so 64 of slack still
	// separates the two by a wide margin.
	const slack = 64

	limit := int64(baseline + 1 + remainderWorkers(stp.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency) + slack)
	require.LessOrEqual(t, peak.Load(), limit, "the lookup phase must not start more goroutines than its worker bound")
}
