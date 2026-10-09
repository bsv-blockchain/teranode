package subtreevalidation

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	aero "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
)

// BenchmarkBlockShapes drives whole blocks of chosen dependency shapes through
// CheckBlockSubtrees, including its ValidateSubtreeInternal pass, followed by the
// record-mined write (SetMinedMulti over every block transaction), on Aerospike
// and Postgres testcontainers, with the real validator and real P2PKH signatures.
// The node is in CATCHINGBLOCKS at a height above any regtest checkpoint.
//
// It only uses APIs that exist on upstream main, so the same file measures the
// code before and after a change. It is skipped unless TERANODE_BLOCK_BENCH is
// set, because it starts containers and takes minutes:
//
//	TERANODE_BLOCK_BENCH=1 go test -run '^$' -bench BenchmarkBlockShapes \
//	    -benchtime 1x -count 5 -timeout 2h ./services/subtreevalidation/
//
// Each iteration builds a fresh block (outside parents stored and mined first,
// untimed) and times the block path plus the record-mined write. Reported:
// tx/s over the block's transactions, and cores busy (this process's CPU time
// over wall time; the store's own CPU runs in its container and is not counted).
func BenchmarkBlockShapes(b *testing.B) {
	if os.Getenv("TERANODE_BLOCK_BENCH") == "" {
		b.Skip("set TERANODE_BLOCK_BENCH=1 to run the block-shape bench (starts containers)")
	}

	storeNames := []string{"aerospike", "postgres"}
	if v := os.Getenv("TERANODE_BLOCK_BENCH_STORES"); v != "" {
		storeNames = strings.Split(v, ",")
	}

	for _, storeName := range storeNames {
		for _, shape := range blockShapes {
			b.Run(storeName+"/"+shape.name, func(b *testing.B) {
				runBlockShape(b, storeName, shape)
			})
		}
	}
}

type blockShape struct {
	name string
	// widths is the number of block transactions per dependency level.
	widths []int
	// extraOutsideShare is the share of transactions above level 0 that also
	// spend an outside parent, which sets the in-block spend share.
	extraOutsideShare float64
	// preCreatedShare of the block's transactions already exist unmined, as
	// propagation leaves them at the tip.
	preCreatedShare float64
	// rootOutputs and rootDataBytes shape the outside parents. More than 128
	// outputs, or a large data-carrier output, stores the parent externally on
	// Aerospike.
	rootOutputs   int
	rootDataBytes int
}

// outputsSpentPerRoot caps how many outputs of one outside parent the block
// spends. Mainnet level-0 transactions rarely share a parent; dozens of
// concurrent spends of one record make Aerospike answer KEY_BUSY. Outside
// parents have exactly this many outputs, except where a shape needs more: an
// unspent output is only extra data for the store to write before the timer
// starts.
const outputsSpentPerRoot = 2

var blockShapes = []blockShape{
	{name: "flat-30000", widths: repeatWidth(30_000, 1), rootOutputs: outputsSpentPerRoot},
	{name: "pr1388-25x64", widths: repeatWidth(64, 25), rootOutputs: outputsSpentPerRoot},
	{name: "deep-344", widths: deepWidths(), extraOutsideShare: 0.053, rootOutputs: outputsSpentPerRoot},
	{name: "tip-90pct-existing", widths: repeatWidth(500, 10), preCreatedShare: 0.9, rootOutputs: outputsSpentPerRoot},
	{name: "external-roots", widths: repeatWidth(400, 10), rootOutputs: 200, rootDataBytes: 40_000},
}

func repeatWidth(width, levels int) []int {
	w := make([]int, levels)
	for i := range w {
		w[i] = width
	}

	return w
}

// deepWidths is 344 levels whose widths stay within 3 to 378, the range of
// median level widths measured on mainnet blocks 949,520 to 949,527. They move
// as a random walk, each level between half and twice the one before, because
// independent draws would put a 3-wide level straight above a 378-wide one, so
// 126 children would spend one parent record at once, and Aerospike refuses
// that many concurrent writes to one record (KEY_BUSY) on either path. The seed
// is fixed so every run builds the same shape.
func deepWidths() []int {
	r := rand.New(rand.NewSource(949520)) //nolint:gosec // deterministic test data
	w := make([]int, 344)
	w[0] = 190

	for i := 1; i < len(w); i++ {
		next := int(float64(w[i-1]) * (0.5 + 1.5*r.Float64()))
		w[i] = min(378, max(3, next))
	}

	return w
}

var (
	benchContainersMu sync.Mutex
	benchStoreURLs    = map[string]*url.URL{}
	benchSeed         uint32
)

// benchStoreURL starts the store's container once per process.
func benchStoreURL(b *testing.B, storeName string) *url.URL {
	benchContainersMu.Lock()
	defer benchContainersMu.Unlock()

	if u, ok := benchStoreURLs[storeName]; ok {
		return u
	}

	ctx := context.Background()

	var raw string

	switch storeName {
	case "aerospike":
		c, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
		require.NoError(b, err)

		host, err := c.Host(ctx)
		require.NoError(b, err)
		port, err := c.ServicePort(ctx)
		require.NoError(b, err)

		waitForBenchAerospike(b, host, port)

		raw = fmt.Sprintf("aerospike://%s:%d/test?set=bench&externalStore=file://./data/externalStore", host, port)
	case "postgres", "utxoset", "utxoset-pertx":
		if u, ok := benchStoreURLs["postgres"]; ok {
			benchStoreURLs[storeName] = u
			return u
		}

		c, err := postgres.Run(ctx, "postgres:16",
			postgres.WithDatabase("bench"), postgres.WithUsername("bench"), postgres.WithPassword("bench"),
			testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Minute)),
		)
		require.NoError(b, err)

		raw, err = c.ConnectionString(ctx, "sslmode=disable")
		require.NoError(b, err)
	}

	u, err := url.Parse(raw)
	require.NoError(b, err)

	benchStoreURLs[storeName] = u
	if storeName != "aerospike" {
		benchStoreURLs["postgres"] = u
	}

	return u
}

// waitForBenchAerospike waits until the namespace accepts writes: Aerospike
// briefly answers FAIL_FORBIDDEN after its port opens.
func waitForBenchAerospike(b *testing.B, host string, port int) {
	client, err := aero.NewClient(host, port)
	require.NoError(b, err)

	defer client.Close()

	key, err := aero.NewKey("test", "bench", []byte("__ready__"))
	require.NoError(b, err)

	policy := aero.NewWritePolicy(0, 0)

	for i := 0; i < 100; i++ {
		if err = client.Put(policy, key, aero.BinMap{"ready": true}); err == nil {
			_, _ = client.Delete(policy, key)
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	require.NoError(b, err, "aerospike namespace never accepted writes")
}

type benchNode struct {
	server *Server
	store  utxo.Store
	key    *bec.PrivateKey
	lock   *bscript.Script
}

const benchHeight = uint32(20_000)

func newBenchNode(b *testing.B, storeName string) *benchNode {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(b)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 1_000_000 // no median-time-past lookups at the bench height
	tSettings.ChainCfgParams = &params
	tSettings.BlockAssembly.Disabled = true

	logBenchSettings(b, tSettings)

	storeURL := benchStoreURL(b, storeName)

	var store utxo.Store

	switch storeName {
	case "aerospike":
		teranodeaerospike.InitPrometheusMetrics()

		s, err := teranodeaerospike.New(ctx, logger, tSettings, storeURL)
		require.NoError(b, err)
		s.SetExternalStore(blobmemory.New())

		store = s
	case "postgres":
		s, err := sql.New(ctx, logger, tSettings, storeURL)
		require.NoError(b, err)

		store = s
	case "utxoset", "utxoset-pertx":
		// The two share one database, so each node starts from empty tables.
		resetBenchUtxoset(b, storeURL)

		s, err := utxoset.New(ctx, logger, tSettings, storeURL)
		require.NoError(b, err)

		store = s
		if storeName == "utxoset-pertx" {
			store = &perTxMulti{Store: s, concurrency: utxo.SpendAndCreateMultiConcurrency(tSettings)}
		}
	}

	b.Cleanup(func() { _ = store.Close(context.Background()) })

	require.NoError(b, store.SetBlockHeight(benchHeight-1))
	require.NoError(b, store.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

	subtreeStore := blobmemory.New()

	localClient, err := blockchain.NewLocalClient(logger, tSettings, &blockchainstore.MockStore{}, subtreeStore, store)
	require.NoError(b, err)

	v, err := validator.New(ctx, logger, tSettings, store, kafka.NewKafkaAsyncProducerMockWithBuffer(1_000_000), kafka.NewKafkaAsyncProducerMock(), nil, nil, nil)
	require.NoError(b, err)

	nilConsumer := &kafka.KafkaConsumerGroup{}
	server, err := New(ctx, logger, tSettings, subtreeStore, blobmemory.New(), store, v,
		&fsmStateOverrideClient{ClientI: localClient, state: blockchain.FSMStateCATCHINGBLOCKS}, nilConsumer, nilConsumer, nil, nil)
	require.NoError(b, err)

	key, err := bec.NewPrivateKey()
	require.NoError(b, err)

	lock, err := bscript.NewP2PKHFromPubKeyBytes(key.PubKey().Compressed())
	require.NoError(b, err)

	return &benchNode{server: server, store: store, key: key, lock: lock}
}

// perTxMulti is the utxoset store writing a list one transaction at a time through the shared
// default, which is what the netted write replaces. Everything else is the same store.
type perTxMulti struct {
	*utxoset.Store
	concurrency int
}

func (p *perTxMulti) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	return utxo.DefaultSpendAndCreateMulti(ctx, p.Store, p.concurrency, txs, blockHeight, opts...)
}

func resetBenchUtxoset(b *testing.B, storeURL *url.URL) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, storeURL.String())
	require.NoError(b, err)

	defer func() { _ = conn.Close(ctx) }()

	_, err = conn.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`)
	require.NoError(b, err)
}

var benchSettingsLogged sync.Once

func logBenchSettings(b *testing.B, s *settings.Settings) {
	benchSettingsLogged.Do(func() {
		b.Logf("bench settings: spendBatcher %d/%dms storeBatcher %d/%dms outpointBatcher %d/%dms getBatcher %d/%dms subtreevalidation_spendBatcherSize %d",
			s.UtxoStore.SpendBatcherSize, s.UtxoStore.SpendBatcherDurationMillis,
			s.UtxoStore.StoreBatcherSize, s.UtxoStore.StoreBatcherDurationMillis,
			s.UtxoStore.OutpointBatcherSize, s.UtxoStore.OutpointBatcherDurationMillis,
			s.UtxoStore.GetBatcherSize, s.UtxoStore.GetBatcherDurationMillis,
			s.SubtreeValidation.SpendBatcherSize)
	})
}

type benchOutput struct {
	tx   *bt.Tx
	vout uint32
}

// buildBenchBlock stores and mines the outside parents, builds the block's
// signed transactions level by level, and returns them in block order.
func buildBenchBlock(b *testing.B, node *benchNode, shape blockShape) []*bt.Tx {
	ctx := context.Background()

	benchSeed++
	seed := benchSeed
	r := rand.New(rand.NewSource(int64(seed))) //nolint:gosec // deterministic test data

	// Outside parents: enough outputs for level 0 plus the extra outside spends.
	outsideNeeded := shape.widths[0]
	for _, w := range shape.widths[1:] {
		outsideNeeded += int(float64(w)*shape.extraOutsideShare) + 1
	}

	var (
		outside []benchOutput
		roots   []*bt.Tx
	)

	for n := 0; len(outside) < outsideNeeded; n++ {
		root := bt.NewTx()
		root.LockTime = seed

		input := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00}), SequenceNumber: 0xffffffff,
			PreviousTxScript: node.lock, PreviousTxSatoshis: uint64(shape.rootOutputs)*100_000_000 + 1_000} //nolint:gosec
		prev := chainhash.Hash{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(n), byte(n >> 8), byte(n >> 16), 0x77}
		require.NoError(b, input.PreviousTxIDAdd(&prev))
		root.Inputs = append(root.Inputs, input)

		for o := 0; o < shape.rootOutputs; o++ {
			root.AddOutput(&bt.Output{Satoshis: 100_000_000, LockingScript: node.lock})
		}

		if shape.rootDataBytes > 0 {
			data := make([]byte, shape.rootDataBytes)
			root.AddOutput(&bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(append([]byte{0x00, 0x6a, 0x4e, byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), byte(len(data) >> 24)}, data...))})
		}

		roots = append(roots, root)

		for o := 0; o < min(outputsSpentPerRoot, shape.rootOutputs); o++ {
			outside = append(outside, benchOutput{tx: root, vout: uint32(o)}) //nolint:gosec
		}
	}

	// Store the outside parents concurrently, so the batchers fill as they do
	// under load; one at a time each would wait out a batcher timer.
	createConcurrently(b, len(roots), func(i int) error {
		_, _, err := node.store.SpendAndCreate(ctx, roots[i], benchHeight-10, utxo.WithCreateOnly(),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1000 + seed, BlockHeight: benchHeight - 10}))

		return err
	})

	nextOutside := 0
	takeOutside := func() benchOutput {
		o := outside[nextOutside]
		nextOutside++

		return o
	}

	var (
		txs       []*bt.Tx
		prevLevel []benchOutput
	)

	getter := &unlocker.Getter{PrivateKey: node.key}

	for level, width := range shape.widths {
		// Every transaction above level 0 spends one output of the level above,
		// round robin, so it sits exactly at this level; the level above has
		// enough outputs for this one.
		outputsPerTx := 2
		if level+1 < len(shape.widths) && shape.widths[level+1] > width {
			outputsPerTx = shape.widths[level+1]/width + 2
		}

		var thisLevel []benchOutput

		for i := 0; i < width; i++ {
			var spends []benchOutput

			if level == 0 {
				spends = append(spends, takeOutside())
			} else {
				spends = append(spends, prevLevel[i%len(prevLevel)])
				if r.Float64() < shape.extraOutsideShare {
					spends = append(spends, takeOutside())
				}
			}

			tx := bt.NewTx()
			tx.LockTime = seed

			var in uint64

			for _, s := range spends {
				require.NoError(b, tx.FromUTXOs(&bt.UTXO{TxIDHash: s.tx.TxIDChainHash(), Vout: s.vout, LockingScript: s.tx.Outputs[s.vout].LockingScript, Satoshis: s.tx.Outputs[s.vout].Satoshis}))
				in += s.tx.Outputs[s.vout].Satoshis
			}

			// Values halve or worse at every level, so a fixed fee would
			// underflow on deep shapes; policy checks are off on the block path.
			fee := min(uint64(300), in/10)
			each := (in - fee) / uint64(outputsPerTx) //nolint:gosec

			for o := 0; o < outputsPerTx; o++ {
				tx.AddOutput(&bt.Output{Satoshis: each, LockingScript: node.lock})
			}

			require.NoError(b, tx.FillAllInputs(ctx, getter))

			txs = append(txs, tx)

			for o := 0; o < outputsPerTx; o++ {
				thisLevel = append(thisLevel, benchOutput{tx: tx, vout: uint32(o)}) //nolint:gosec
			}
		}

		// Order the level's outputs so round robin over the next level takes
		// output 0 of every transaction first, spreading children across
		// parents. The level has more outputs than the next level has
		// transactions, so no output is spent twice.
		if len(thisLevel) > 0 {
			stride := outputsPerTx
			reordered := make([]benchOutput, 0, len(thisLevel))

			for o := 0; o < stride; o++ {
				for i := o; i < len(thisLevel); i += stride {
					reordered = append(reordered, thisLevel[i])
				}
			}

			thisLevel = reordered
		}

		prevLevel = thisLevel
	}

	// The tip shape: propagation already created a share of the block,
	// interleaved, so the block path skips them in its pre-check.
	if shape.preCreatedShare > 0 {
		start := 0

		for _, width := range shape.widths {
			level := txs[start : start+width]
			start += width

			createConcurrently(b, len(level), func(i int) error {
				if float64(i%10) >= shape.preCreatedShare*10 {
					return nil
				}

				_, _, err := node.store.SpendAndCreate(ctx, level[i], benchHeight-1)

				return err
			})
		}
	}

	return txs
}

// storeBenchBlock writes one subtree [coinbase placeholder, txs...] and its data,
// and returns a block whose header commits to it.
func storeBenchBlock(b *testing.B, node *benchNode, txs []*bt.Tx) *model.Block {
	ctx := context.Background()

	leaves := 1
	for leaves < len(txs)+1 {
		leaves <<= 1
	}

	st, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(b, err)
	require.NoError(b, st.AddCoinbaseNode())

	data := make([]byte, 0, len(txs)*300)

	for _, tx := range txs {
		require.NoError(b, st.AddNode(*tx.TxIDChainHash(), 300, uint64(tx.Size()))) //nolint:gosec
		data = append(data, tx.Bytes()...)
	}

	subtreeHash := *st.RootHash()

	stBytes, err := st.Serialize()
	require.NoError(b, err)
	require.NoError(b, node.server.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, stBytes))
	require.NoError(b, node.server.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, data))

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(b, err)

	merkleRoot, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(b, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(b, err)

	header := &model.BlockHeader{Version: 0x20000000, HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: merkleRoot, Timestamp: uint32(time.Now().Unix()), Bits: *bits} //nolint:gosec

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{&subtreeHash}, uint64(len(txs)+1), uint64(len(data)), benchHeight, 0) //nolint:gosec
	require.NoError(b, err)

	return block
}

func createConcurrently(b *testing.B, n int, create func(i int) error) {
	var g errgroup.Group

	g.SetLimit(512)

	for i := 0; i < n; i++ {
		g.Go(func() error { return create(i) })
	}

	require.NoError(b, g.Wait())
}

// histogramSum returns the sum of every sample of a Prometheus histogram in the
// default registry, zero when it has none yet.
func histogramSum(b *testing.B, name string) float64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(b, err)

	sum := 0.0

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

		for _, m := range mf.GetMetric() {
			sum += m.GetHistogram().GetSampleSum()
		}
	}

	return sum
}

// labeledHistogramSum is histogramSum restricted to samples whose label has the
// given value.
func labeledHistogramSum(tb testing.TB, name, label, value string) float64 {
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(tb, err)

	sum := 0.0

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}

		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					sum += m.GetHistogram().GetSampleSum()
				}
			}
		}
	}

	return sum
}

func cpuTime() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func runBlockShape(b *testing.B, storeName string, shape blockShape) {
	node := newBenchNode(b, storeName)
	ctx := context.Background()

	var (
		totalTxs  int
		totalWall time.Duration
		totalCPU  time.Duration
		totalGCs  uint32
		// totalPause is stop-the-world GC pause time inside the timed part.
		totalPause time.Duration
		// The timed part split: CheckBlockSubtrees, the ValidateSubtreeInternal
		// pass inside it, and the record-mined write after it.
		totalCheckBlock time.Duration
		totalSecondPass float64
		totalMinedWrite time.Duration
		// stepSums is time per step of the batch path; it stays zero on code
		// that has no batch path.
		stepSums = map[string]float64{}
	)

	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		txs := buildBenchBlock(b, node, shape)
		block := storeBenchBlock(b, node, txs)

		blockBytes, err := block.Bytes()
		require.NoError(b, err)

		hashes := make([]*chainhash.Hash, len(txs))
		for n, tx := range txs {
			hashes[n] = tx.TxIDChainHash()
		}

		var msBefore runtime.MemStats
		runtime.ReadMemStats(&msBefore)

		cpuStart := cpuTime()
		start := time.Now()

		b.StartTimer()

		secondPassBefore := histogramSum(b, "teranode_subtreevalidation_validate_subtree")

		for _, step := range []string{"precheck", "resolve", "check", "check_after_reads", "write", "fallback"} {
			stepSums[step] -= labeledHistogramSum(b, "teranode_subtreevalidation_batch_step", "step", step)
		}

		_, err = node.server.CheckBlockSubtrees(ctx, &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "legacy"})
		require.NoError(b, err)

		checkBlockDone := time.Now()

		for _, step := range []string{"precheck", "resolve", "check", "check_after_reads", "write", "fallback"} {
			stepSums[step] += labeledHistogramSum(b, "teranode_subtreevalidation_batch_step", "step", step)
		}
		totalCheckBlock += checkBlockDone.Sub(start)
		totalSecondPass += histogramSum(b, "teranode_subtreevalidation_validate_subtree") - secondPassBefore

		_, err = node.store.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{BlockID: 5000 + benchSeed, BlockHeight: benchHeight, SubtreeIdx: 0})
		require.NoError(b, err)

		b.StopTimer()

		totalMinedWrite += time.Since(checkBlockDone)
		totalWall += time.Since(start)
		totalCPU += cpuTime() - cpuStart
		totalTxs += len(txs)

		var msAfter runtime.MemStats
		runtime.ReadMemStats(&msAfter)

		totalGCs += msAfter.NumGC - msBefore.NumGC
		totalPause += time.Duration(msAfter.PauseTotalNs - msBefore.PauseTotalNs)
	}

	b.ReportMetric(float64(totalTxs)/totalWall.Seconds(), "tx/s")
	b.ReportMetric(float64(totalCPU)/float64(totalWall), "cores")
	b.ReportMetric(float64(totalTxs)/float64(b.N), "txs/block")
	b.ReportMetric(float64(totalGCs)/float64(b.N), "gcs/block")
	b.ReportMetric(float64(totalPause.Microseconds())/float64(b.N), "gcpause_us/block")
	b.ReportMetric(totalCheckBlock.Seconds()/float64(b.N), "checkblock_s/block")
	b.ReportMetric(totalSecondPass/float64(b.N), "secondpass_s/block")
	b.ReportMetric(totalMinedWrite.Seconds()/float64(b.N), "minedwrite_s/block")

	for step, sum := range stepSums {
		b.ReportMetric(sum/float64(b.N), "batch_"+step+"_s/block")
	}
}
