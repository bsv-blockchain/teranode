package aerospike_test

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// initAerospikeBench initializes Aerospike for benchmarks
func initAerospikeBench(b *testing.B, settings *settings.Settings, logger ulogger.Logger) (*teranode_aerospike.Store, context.Context, func()) {
	teranode_aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	var (
		containerAny any
		err          error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.NewError("container startup panic: %v", r)
			}
		}()
		c, e := aeroTest.RunContainer(ctx)
		if e != nil {
			err = e
			return
		}
		containerAny = c
	}()
	if err != nil {
		b.Skipf("Skipping Aerospike benchmark: container not available (%v)", err)
	}

	type containerAPI interface {
		Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
		Host(ctx context.Context) (string, error)
		ServicePort(ctx context.Context) (int, error)
	}

	container, ok := containerAny.(containerAPI)
	if !ok {
		b.Fatal("Aerospike benchmark: unexpected container type")
	}

	host, err := container.Host(ctx)
	require.NoError(b, err)

	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	client, aeroErr := uaerospike.NewClient(host, port)
	require.NoError(b, aeroErr)

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&block_retention=%d&externalStore=file://./data/externalStore",
		host, port, aerospikeNamespace, aerospikeSet, aerospikeBlockRetention)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(b, err)

	db, err := teranode_aerospike.New(ctx, logger, settings, aeroURL)
	require.NoError(b, err)

	db.SetExternalStore(memory.New())

	deferFn := func() {
		client.Close()
		_ = container.Terminate(ctx)
	}

	return db, ctx, deferFn
}

// BenchmarkSetMinedMulti benchmarks SetMinedMulti with both Lua UDF and filter expressions
// Run with: go test -bench=BenchmarkSetMinedMulti -benchmem -tags aerospike ./stores/utxo/aerospike/
func BenchmarkSetMinedMulti(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	logger := ulogger.NewErrorTestLogger(b)
	tSettings := test.CreateBaseTestSettings(b)

	// Test with different batch sizes
	batchSizes := []int{1, 10, 50, 100}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("Lua_BatchSize_%d", batchSize), func(b *testing.B) {
			benchmarkSetMinedMultiWithMode(b, tSettings, logger, batchSize, false)
		})

		b.Run(fmt.Sprintf("Expressions_BatchSize_%d", batchSize), func(b *testing.B) {
			benchmarkSetMinedMultiWithMode(b, tSettings, logger, batchSize, true)
		})
	}
}

func benchmarkSetMinedMultiWithMode(b *testing.B, tSettings *settings.Settings, logger ulogger.Logger, batchSize int, useExpressions bool) {
	tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpressions

	store, ctx, deferFn := initAerospikeBench(b, tSettings, logger)
	defer deferFn()

	// Create transactions - use coinbase tx to avoid fee validation issues
	txHashes := make([]*chainhash.Hash, batchSize)
	for i := 0; i < batchSize; i++ {
		txLocal, err := bt.NewTxFromString(coinbaseTx.String())
		require.NoError(b, err)
		// Modify output to make each transaction unique
		txLocal.Outputs[0].Satoshis = txLocal.Outputs[0].Satoshis + uint64(i)

		_, err = store.Create(ctx, txLocal, 0)
		require.NoError(b, err)

		txHashes[i] = txLocal.TxIDChainHash()
	}

	minedBlockInfo := utxo.MinedBlockInfo{
		BlockID:        uint32(100),
		BlockHeight:    uint32(1000),
		SubtreeIdx:     0,
		OnLongestChain: true,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		minedBlockInfo.BlockID = uint32(100 + i)
		minedBlockInfo.BlockHeight = uint32(1000 + i)

		_, err := store.SetMinedMulti(ctx, txHashes, minedBlockInfo)
		if err != nil {
			b.Fatalf("SetMinedMulti failed: %v", err)
		}
	}
}

// BenchmarkSetMinedMultiAlreadyMined benchmarks the filtered-out case
func BenchmarkSetMinedMultiAlreadyMined(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	logger := ulogger.NewErrorTestLogger(b)
	tSettings := test.CreateBaseTestSettings(b)

	batchSize := 100

	b.Run("Lua_AlreadyMined", func(b *testing.B) {
		benchmarkSetMinedMultiAlreadyMinedWithMode(b, tSettings, logger, batchSize, false)
	})

	b.Run("Expressions_AlreadyMined", func(b *testing.B) {
		benchmarkSetMinedMultiAlreadyMinedWithMode(b, tSettings, logger, batchSize, true)
	})
}

func benchmarkSetMinedMultiAlreadyMinedWithMode(b *testing.B, tSettings *settings.Settings, logger ulogger.Logger, batchSize int, useExpressions bool) {
	tSettings.Aerospike.EnableSetMinedFilterExpressions = useExpressions

	store, ctx, deferFn := initAerospikeBench(b, tSettings, logger)
	defer deferFn()

	txHashes := make([]*chainhash.Hash, batchSize)
	for i := 0; i < batchSize; i++ {
		txLocal, err := bt.NewTxFromString(coinbaseTx.String())
		require.NoError(b, err)
		txLocal.Outputs[0].Satoshis = txLocal.Outputs[0].Satoshis + uint64(i)

		_, err = store.Create(ctx, txLocal, 0)
		require.NoError(b, err)

		txHashes[i] = txLocal.TxIDChainHash()
	}

	minedBlockInfo := utxo.MinedBlockInfo{
		BlockID:        uint32(500),
		BlockHeight:    uint32(1500),
		SubtreeIdx:     0,
		OnLongestChain: true,
	}

	_, err := store.SetMinedMulti(ctx, txHashes, minedBlockInfo)
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.SetMinedMulti(ctx, txHashes, minedBlockInfo)
		require.NoError(b, err)
	}
}
