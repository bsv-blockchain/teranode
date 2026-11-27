package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The reorg scenario is the fork shape of issue 2269 built at any size: block assembly sits on the
// lost block B with a pending set P, and a reorg moves B back and the winning blocks F1..Fn forward
// (normally two: since fac8a23fd a fork reorg starts when the competitor is extended). It runs
// through reorgBlocks, the production call site, over a real sqlitememory blockchain and UTXO store.
//
// The sets are built from the config:
//   - B: lost transactions; a lostInWinners fraction of them is also in F, the rest (raw D1) is not.
//     lostAlsoPending of the raw D1 transactions are also pending (the X case of design section 5
//     item 2: excluded from the built prefix, kept at their pending position, marked false).
//   - P: pending transactions; a pendingInWinners fraction (the hits) is in F, at the front of P
//     when hitsAtFront is set (the winner takes the oldest transactions), spread through it otherwise.
//   - F: B's shared transactions, the hits and winnerOnly transactions no one else has, shuffled and
//     split across the forward blocks.
//   - Parents inside the scenario, so the order of the result matters: pendingChains pending
//     transactions also spend the pending transaction before them, and pendingSpendsLost pending
//     transactions spend a raw D1 transaction (so D1 must come first after the reorg). Neither
//     kind of child is ever a hit unless its parent is.
//
// Raw D1 transactions are real transactions with records in the UTXO store, because a reorg marks
// them not on the longest chain and the store treats a missing record there as fatal. Every other
// transaction is synthetic: its hash and the confirmed parent it spends are pure functions of its id,
// so the scenario keeps 8 bytes per transaction per order and builds hundreds of millions of them
// (the reorgscale suite). The hash sets behind the expectations are built only when a test asks.
type reorgScenarioConfig struct {
	seed              uint64
	pending           int
	lost              int
	lostInWinners     float64
	pendingInWinners  float64
	hitsAtFront       bool
	winnerOnly        int
	forwardBlocks     int
	lostAlsoPending   int
	pendingChains     int
	pendingSpendsLost int
	subtreeSize       int
	diskTxMap         bool

	// skipStoreMarks counts the longest-chain marks instead of sending them to the UTXO store, and
	// creates no raw D1 records. The reorgscale suite sets it to time the processor's phases; the
	// marks are measured on their own against Aerospike.
	skipStoreMarks bool

	// reorgStoreDeadline sets blockassembly_reorgStoreDeadline.
	reorgStoreDeadline time.Duration

	// storeConsumer, when set, answers every store request in place of the default listener,
	// which answers ErrChan and Result as stored.
	storeConsumer func(req NewSubtreeRequest)

	// storedSet, when set, is the processor's stored set (WithStoredSet); the consumer fills it.
	storedSet *StoredSet

	// storeQueue, when set, is the processor's store queue (WithStoreQueue), which the test drains;
	// no consumer of newSubtreeChan sees a request then.
	storeQueue *StoreQueue
}

type reorgScenario struct {
	cfg       reorgScenarioConfig
	stp       *SubtreeProcessor
	utxo      *markSpyStore
	blobStore *blob_memory.Memory
	client    blockchain.ClientI
	genesis   *model.BlockHeader
	lost      *model.Block
	forward   []*model.Block

	// release stops the processor; it runs once, at the latest at the end of the test.
	release func()

	lostIDs    []scenarioID
	pendingIDs []scenarioID
	winnerIDs  []scenarioID
	rawD1Txs   []*bt.Tx

	// extraParent is the second parent of a pending transaction that spends another scenario
	// transaction, with the output it spends.
	extraParent map[scenarioID]scenarioOutpoint

	setsOnce   sync.Once
	lostSet    map[chainhash.Hash]struct{}
	pendingSet map[chainhash.Hash]struct{}
	winnerSet  map[chainhash.Hash]struct{}
}

// scenarioID names a scenario transaction: its kind in the top four bits, its index below.
type scenarioID uint64

const (
	kindShift                 = 60
	kindLostShared scenarioID = iota << kindShift
	kindRawD1
	kindPending
	kindWinnerOnly
	kindIntake
)

func (id scenarioID) kind() scenarioID { return id & (0xf << kindShift) }
func (id scenarioID) index() int       { return int(id &^ (0xf << kindShift)) }

// confirmedParentMarker tags the last 8 bytes of every confirmed parent hash, so a confirmed parent
// is recognised without a set of them.
const confirmedParentMarker = 0xc0ffee2269c0ffee

func isConfirmedParent(h chainhash.Hash) bool {
	return binary.LittleEndian.Uint64(h[24:]) == confirmedParentMarker
}

type scenarioOutpoint struct {
	parent scenarioID
	vout   uint32
}

// scenarioTx is a node of the scenario with the parents it spends.
type scenarioTx struct {
	node     subtreepkg.Node
	inpoints subtreepkg.TxInpoints
}

// splitmix64 gives well-spread hashes cheaply; SortedHashSet partitions by the top bits, so
// counter-shaped hashes would all land in one partition.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb

	return x ^ (x >> 31)
}

func scenarioHash(seed, tag uint64) chainhash.Hash {
	var h chainhash.Hash

	base := splitmix64(seed*0x100000001b3 ^ tag)
	for i := range 4 {
		binary.LittleEndian.PutUint64(h[i*8:], splitmix64(base+uint64(i)))
	}

	return h
}

func (sc *reorgScenario) confirmedParent(id scenarioID) chainhash.Hash {
	h := scenarioHash(sc.cfg.seed^0x5bd1e995, uint64(id))
	binary.LittleEndian.PutUint64(h[24:], confirmedParentMarker)

	return h
}

// hash is the transaction hash of id.
func (sc *reorgScenario) hash(id scenarioID) chainhash.Hash {
	if id.kind() == kindRawD1 {
		return *sc.rawD1Txs[id.index()].TxIDChainHash()
	}

	return scenarioHash(sc.cfg.seed, uint64(id))
}

// tx is the node and inpoints of id.
func (sc *reorgScenario) tx(t testing.TB, id scenarioID) scenarioTx {
	if id.kind() == kindRawD1 {
		real := sc.rawD1Txs[id.index()]

		inpoints, err := subtreepkg.NewTxInpointsFromTx(real)
		require.NoError(t, err)

		return scenarioTx{node: subtreepkg.Node{Hash: *real.TxIDChainHash(), Fee: 200, SizeInBytes: uint64(real.Size())}, inpoints: inpoints}
	}

	return scenarioTx{
		node:     subtreepkg.Node{Hash: sc.hash(id), Fee: 200, SizeInBytes: 250},
		inpoints: sc.inpoints(id),
	}
}

// inpoints is what a synthetic transaction spends: output 0 of its confirmed parent and, when it
// has one, its extra parent's output.
func (sc *reorgScenario) inpoints(id scenarioID) subtreepkg.TxInpoints {
	extra, ok := sc.extraParent[id]
	if !ok {
		return voutZeroInpoints(sc.confirmedParent(id))
	}

	return subtreepkg.NewTxInpointsFromPacked(
		[]chainhash.Hash{sc.confirmedParent(id), sc.hash(extra.parent)},
		[]uint32{1, 0, 1, extra.vout},
	)
}

// rawD1Tx is a real transaction, unique to i, spending its confirmed parent.
func (sc *reorgScenario) rawD1Tx(t testing.TB, i int) *bt.Tx {
	parent := sc.confirmedParent(kindRawD1 | scenarioID(i))

	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 10_000, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, in.PreviousTxIDAdd(&parent))

	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], sc.cfg.seed<<40^uint64(i))

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{in}
	tx.Outputs = []*bt.Output{{Satoshis: 9_000, LockingScript: bscript.NewFromBytes(append([]byte{0x6a, 0x08}, data[:]...))}}

	return tx
}

func newReorgScenario(t testing.TB, cfg reorgScenarioConfig) *reorgScenario {
	t.Helper()

	if cfg.forwardBlocks == 0 {
		cfg.forwardBlocks = 2
	}

	if cfg.subtreeSize == 0 {
		cfg.subtreeSize = 1024
	}

	sc := &reorgScenario{cfg: cfg}
	rng := rand.New(rand.NewPCG(cfg.seed, 0x2269))

	lostShared := int(math.Round(float64(cfg.lost) * cfg.lostInWinners))
	rawD1Count := cfg.lost - lostShared
	require.LessOrEqual(t, cfg.lostAlsoPending, rawD1Count, "lostAlsoPending transactions are part of raw D1")

	sc.rawD1Txs = make([]*bt.Tx, rawD1Count)
	for i := range rawD1Count {
		sc.rawD1Txs[i] = sc.rawD1Tx(t, i)
	}

	// The lost block, shuffled.
	sc.lostIDs = make([]scenarioID, 0, cfg.lost)
	for i := range lostShared {
		sc.lostIDs = append(sc.lostIDs, kindLostShared|scenarioID(i))
	}

	for i := range rawD1Count {
		sc.lostIDs = append(sc.lostIDs, kindRawD1|scenarioID(i))
	}

	rng.Shuffle(len(sc.lostIDs), func(i, j int) { sc.lostIDs[i], sc.lostIDs[j] = sc.lostIDs[j], sc.lostIDs[i] })

	// The pending set, with the first lostAlsoPending raw D1 transactions spread through it.
	sc.pendingIDs = make([]scenarioID, 0, cfg.pending)
	synthetic := cfg.pending - cfg.lostAlsoPending

	for i, x := 0, 0; i < synthetic || x < cfg.lostAlsoPending; {
		if x < cfg.lostAlsoPending && i >= (x+1)*synthetic/(cfg.lostAlsoPending+1) {
			sc.pendingIDs = append(sc.pendingIDs, kindRawD1|scenarioID(x))
			x++

			continue
		}

		sc.pendingIDs = append(sc.pendingIDs, kindPending|scenarioID(i))
		i++
	}

	sc.addParentEdges(t, synthetic, rawD1Count)

	// The hits: pending transactions the winners mined. Never a lostAlsoPending transaction, never a
	// child of a raw D1 transaction (the winners do not have its parent), and never a chain child
	// whose parent might not be a hit.
	hitCount := int(math.Round(float64(cfg.pending) * cfg.pendingInWinners))
	candidates := make([]uint32, 0, synthetic)

	for pos, id := range sc.pendingIDs {
		if id.kind() != kindPending {
			continue
		}

		if extra, ok := sc.extraParent[id]; ok && (extra.parent.kind() == kindRawD1 || !cfg.hitsAtFront) {
			continue
		}

		candidates = append(candidates, uint32(pos))
	}

	if !cfg.hitsAtFront {
		rng.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	}

	require.LessOrEqual(t, hitCount, len(candidates))

	// The winners, shuffled: B's shared part, the hits and the winner-only transactions.
	sc.winnerIDs = make([]scenarioID, 0, lostShared+hitCount+cfg.winnerOnly)
	for i := range lostShared {
		sc.winnerIDs = append(sc.winnerIDs, kindLostShared|scenarioID(i))
	}

	for _, pos := range candidates[:hitCount] {
		sc.winnerIDs = append(sc.winnerIDs, sc.pendingIDs[pos])
	}

	for i := range cfg.winnerOnly {
		sc.winnerIDs = append(sc.winnerIDs, kindWinnerOnly|scenarioID(i))
	}

	rng.Shuffle(len(sc.winnerIDs), func(i, j int) { sc.winnerIDs[i], sc.winnerIDs[j] = sc.winnerIDs[j], sc.winnerIDs[i] })
	sc.winnerIDs = sc.parentsFirst(sc.winnerIDs)

	sc.buildChain(t)
	sc.buildProcessor(t)

	return sc
}

// parentsFirst keeps ids in their order, except that a transaction whose scenario parent is also in
// ids follows that parent directly. The winning blocks are consecutive slices of the result, so a
// child is never in an earlier block than its parent, nor before it in the same block.
func (sc *reorgScenario) parentsFirst(ids []scenarioID) []scenarioID {
	inIDs := make(map[scenarioID]struct{}, len(ids))
	for _, id := range ids {
		inIDs[id] = struct{}{}
	}

	waiting := make(map[scenarioID][]scenarioID)
	out := make([]scenarioID, 0, len(ids))

	var emit func(id scenarioID)

	emit = func(id scenarioID) {
		out = append(out, id)

		for _, child := range waiting[id] {
			emit(child)
		}
	}

	for _, id := range ids {
		if extra, ok := sc.extraParent[id]; ok {
			if _, parentInIDs := inIDs[extra.parent]; parentInIDs {
				waiting[extra.parent] = append(waiting[extra.parent], id)
				continue
			}
		}

		emit(id)
	}

	return out
}

// addParentEdges picks the pending transactions that spend another scenario transaction: first the
// children of raw D1 transactions that are not pending, then the chain children, each spending the
// synthetic pending transaction before it. A transaction gets at most one extra parent, and a chain
// parent is never a D1 child, so with hits at the front a hit chain child always has a hit parent.
func (sc *reorgScenario) addParentEdges(t testing.TB, synthetic, rawD1Count int) {
	t.Helper()

	cfg := sc.cfg
	sc.extraParent = make(map[scenarioID]scenarioOutpoint, cfg.pendingChains+cfg.pendingSpendsLost)

	require.LessOrEqual(t, cfg.pendingSpendsLost, rawD1Count-cfg.lostAlsoPending, "each D1 parent has one child")
	require.Less(t, 2*(cfg.pendingChains+cfg.pendingSpendsLost), synthetic, "room for the edges")

	free := func(i int) int {
		for {
			_, taken := sc.extraParent[kindPending|scenarioID(i)]
			if !taken {
				return i
			}

			// index 0 has no transaction before it to spend
			i = max(1, (i+1)%synthetic)
		}
	}

	for j := range cfg.pendingSpendsLost {
		child := free((2*j + 1) * synthetic / (2 * cfg.pendingSpendsLost))
		sc.extraParent[kindPending|scenarioID(child)] = scenarioOutpoint{kindRawD1 | scenarioID(cfg.lostAlsoPending+j), 0}
	}

	for j := range cfg.pendingChains {
		child := max(1, (j+1)*synthetic/(cfg.pendingChains+1))

		for {
			child = free(max(1, child))
			_, parentIsChild := sc.extraParent[kindPending|scenarioID(child-1)]

			if !parentIsChild {
				break
			}

			child = (child + 1) % synthetic
		}

		sc.extraParent[kindPending|scenarioID(child)] = scenarioOutpoint{kindPending | scenarioID(child-1), 1}
	}
}

// buildChain stores the blocks' subtrees and the chain genesis -> B and genesis -> F1 -> .. -> Fn,
// and creates the UTXO records the reorg needs: raw D1 and B's coinbase.
func (sc *reorgScenario) buildChain(t testing.TB) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	blockchainStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	sc.client, err = blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	sc.genesis, _, err = sc.client.GetBestBlockHeader(ctx)
	require.NoError(t, err)

	sc.blobStore = blob_memory.New()

	nonce := uint32(sc.cfg.seed%1000)*100 + 1
	sc.lost = sc.storeBlock(t, 1, childHeader(sc.genesis, nonce), sc.lostIDs, true)
	require.NoError(t, sc.client.AddBlock(ctx, sc.lost, "", blockchainoptions.WithMinedSet(true)))

	parent := sc.genesis
	sc.forward = make([]*model.Block, 0, sc.cfg.forwardBlocks)

	for i := range sc.cfg.forwardBlocks {
		lo, hi := i*len(sc.winnerIDs)/sc.cfg.forwardBlocks, (i+1)*len(sc.winnerIDs)/sc.cfg.forwardBlocks
		block := sc.storeBlock(t, uint32(i+1), childHeader(parent, nonce+uint32(i)+1), sc.winnerIDs[lo:hi], false)
		require.NoError(t, sc.client.AddBlock(ctx, block, "", blockchainoptions.WithMinedSet(true)))
		sc.forward = append(sc.forward, block)
		parent = block.Header
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///reorgscenario")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	sc.utxo = &markSpyStore{Store: sqlStore, countOnly: sc.cfg.skipStoreMarks}

	for _, tx := range sc.rawD1Txs {
		if sc.cfg.skipStoreMarks {
			break
		}

		_, err = sqlStore.Create(ctx, tx, sc.lost.Height)
		require.NoError(t, err)
	}

	_, _, err = sqlStore.SpendAndCreate(ctx, sc.lost.CoinbaseTx, sc.lost.Height, utxo.WithCreateOnly())
	require.NoError(t, err)
}

// storeBlock stores ids as the block's subtrees (coinbase placeholder first) and, with meta set,
// their meta files: a move-back reads the lost block's meta for the parents.
func (sc *reorgScenario) storeBlock(t testing.TB, height uint32, header *model.BlockHeader, ids []scenarioID, meta bool) *model.Block {
	t.Helper()

	ctx := context.Background()
	size := sc.cfg.subtreeSize
	block := &model.Block{
		Height:           height,
		Header:           header,
		CoinbaseTx:       scenarioCoinbase(t, height, uint64(header.Nonce)),
		TransactionCount: uint64(len(ids) + 1),
		SizeInBytes:      uint64(len(ids)*250 + 200),
		Subtrees:         []*chainhash.Hash{},
	}

	// The first subtree holds the coinbase placeholder and size-1 transactions.
	for start, first := 0, true; first || start < len(ids); first = false {
		st, err := subtreepkg.NewTreeByLeafCount(size)
		require.NoError(t, err)

		var stMeta *subtreepkg.Meta
		if meta {
			stMeta = subtreepkg.NewSubtreeMeta(st)
		}

		end := min(start+size, len(ids))
		if first {
			require.NoError(t, st.AddCoinbaseNode())

			if meta {
				require.NoError(t, stMeta.SetTxInpoints(0, subtreepkg.NewTxInpoints()))
			}

			end = min(start+size-1, len(ids))
		}

		for _, id := range ids[start:end] {
			tx := sc.tx(t, id)
			require.NoError(t, st.AddSubtreeNode(tx.node))

			if meta {
				require.NoError(t, stMeta.SetTxInpoints(st.Length()-1, tx.inpoints))
			}
		}

		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)
		require.NoError(t, sc.blobStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

		if meta {
			metaBytes, err := stMeta.Serialize()
			require.NoError(t, err)
			require.NoError(t, sc.blobStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeMeta, metaBytes))
		}

		block.Subtrees = append(block.Subtrees, st.RootHash())
		start = end
	}

	return block
}

// buildProcessor starts block assembly on the lost block with the pending set loaded in order.
func (sc *reorgScenario) buildProcessor(t testing.TB) {
	t.Helper()

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = sc.cfg.subtreeSize
	tSettings.BlockAssembly.UseDynamicSubtreeSize = false
	tSettings.BlockAssembly.ReorgStoreDeadline = sc.cfg.reorgStoreDeadline

	var opts []Options
	if sc.cfg.diskTxMap {
		opts = append(opts, WithTxMapDirs([]string{t.TempDir()}))
	}

	if sc.cfg.storedSet != nil {
		opts = append(opts, WithStoredSet(sc.cfg.storedSet))
	}

	if sc.cfg.storeQueue != nil {
		opts = append(opts, WithStoreQueue(sc.cfg.storeQueue))
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 1024)
	listenerDone := make(chan struct{})

	go func() {
		defer close(listenerDone)

		consume := sc.cfg.storeConsumer
		if consume == nil {
			consume = answerStored
		}

		for req := range newSubtreeChan {
			consume(req)
		}
	}()

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, sc.blobStore, sc.client, sc.utxo, newSubtreeChan, opts...)
	require.NoError(t, err)

	sc.release = sync.OnceFunc(func() {
		stp.Stop(context.Background())
		close(newSubtreeChan)
		<-listenerDone
	})
	t.Cleanup(sc.release)

	stp.InitCurrentBlockHeader(sc.lost.Header)

	const batchSize = 1 << 16

	for start := 0; start < len(sc.pendingIDs); start += batchSize {
		ids := sc.pendingIDs[start:min(start+batchSize, len(sc.pendingIDs))]
		txs := make([]scenarioTx, len(ids))
		batch := make([]*utxo.UnminedTransaction, len(ids))

		for i, id := range ids {
			txs[i] = sc.tx(t, id)
			batch[i] = &utxo.UnminedTransaction{Node: &txs[i].node, TxInpoints: &txs[i].inpoints, CreatedAt: start + i}
		}

		require.NoError(t, stp.AddNodesDirectly(batch, true))
	}

	sc.stp = stp
}

// startIntake feeds new transactions into the processor's queue at about tps per second, in
// batches, until stop is called. stop returns how many were sent. A reorg drains the queue, so this
// is the live intake a reorg competes with.
func (sc *reorgScenario) startIntake(tps int) (stop func() int) {
	const batch = 1000

	done := make(chan struct{})
	sent := make(chan int, 1)

	go func() {
		ticker := time.NewTicker(time.Second * batch / time.Duration(tps))
		defer ticker.Stop()

		total := 0

		for {
			select {
			case <-done:
				sent <- total
				return
			case <-ticker.C:
				nodes := make([]subtreepkg.Node, batch)
				inpoints := make([]*subtreepkg.TxInpoints, batch)

				for i := range batch {
					id := kindIntake | scenarioID(total+i)
					p := voutZeroInpoints(sc.confirmedParent(id))
					nodes[i] = subtreepkg.Node{Hash: sc.hash(id), Fee: 200, SizeInBytes: 250}
					inpoints[i] = &p
				}

				sc.stp.AddBatch(nodes, inpoints)
				total += batch
			}
		}
	}()

	return func() int {
		close(done)
		return <-sent
	}
}

// answerStored answers a store request as a store that stored it.
func answerStored(req NewSubtreeRequest) {
	if req.Result != nil {
		req.Result <- StoreResult{SubtreeStored: true, MetaNotApplicable: true}
	}

	if req.ErrChan != nil {
		req.ErrChan <- nil
	}
}

// run moves B back and the winning blocks forward through reorgBlocks.
func (sc *reorgScenario) run(t testing.TB) error {
	t.Helper()

	return sc.stp.reorgBlocks(context.Background(), []*model.Block{sc.lost}, sc.forward)
}

func (sc *reorgScenario) hashes(ids []scenarioID) []chainhash.Hash {
	out := make([]chainhash.Hash, len(ids))
	for i, id := range ids {
		out[i] = sc.hash(id)
	}

	return out
}

func (sc *reorgScenario) hashSet(ids []scenarioID) map[chainhash.Hash]struct{} {
	out := make(map[chainhash.Hash]struct{}, len(ids))
	for _, id := range ids {
		out[sc.hash(id)] = struct{}{}
	}

	return out
}

func (sc *reorgScenario) ensureSets() {
	sc.setsOnce.Do(func() {
		sc.lostSet = sc.hashSet(sc.lostIDs)
		sc.pendingSet = sc.hashSet(sc.pendingIDs)
		sc.winnerSet = sc.hashSet(sc.winnerIDs)
	})
}

func (sc *reorgScenario) inWinners(h chainhash.Hash) bool {
	sc.ensureSets()
	_, ok := sc.winnerSet[h]

	return ok
}

func (sc *reorgScenario) inPending(h chainhash.Hash) bool {
	sc.ensureSets()
	_, ok := sc.pendingSet[h]

	return ok
}

func (sc *reorgScenario) inLost(h chainhash.Hash) bool {
	sc.ensureSets()
	_, ok := sc.lostSet[h]

	return ok
}

// lostOrder is B's transactions in B's order, and pendingOrder P's in P's order.
func (sc *reorgScenario) lostOrder() []chainhash.Hash    { return sc.hashes(sc.lostIDs) }
func (sc *reorgScenario) pendingOrder() []chainhash.Hash { return sc.hashes(sc.pendingIDs) }

// filter returns the hashes of order for which keep holds, in order.
func filter(order []chainhash.Hash, keep func(chainhash.Hash) bool) []chainhash.Hash {
	out := make([]chainhash.Hash, 0)

	for _, h := range order {
		if keep(h) {
			out = append(out, h)
		}
	}

	return out
}

// rawD1 is B minus F, in B's order.
func (sc *reorgScenario) rawD1() []chainhash.Hash {
	return filter(sc.lostOrder(), func(h chainhash.Hash) bool { return !sc.inWinners(h) })
}

// hits are the pending transactions the winners mined, in P's order.
func (sc *reorgScenario) hits() []chainhash.Hash {
	return filter(sc.pendingOrder(), sc.inWinners)
}

// winnerNotLost is F minus B (D2), the set a reorg marks on the longest chain.
func (sc *reorgScenario) winnerNotLost() []chainhash.Hash {
	return filter(sc.hashes(sc.winnerIDs), func(h chainhash.Hash) bool { return !sc.inLost(h) })
}

// expectedOrderAfter is the order design section 1 owes after the reorg: effective D1 (raw D1 minus
// the pending transactions) in B's order, then P minus F in P's order.
func (sc *reorgScenario) expectedOrderAfter() []chainhash.Hash {
	effectiveD1 := filter(sc.lostOrder(), func(h chainhash.Hash) bool { return !sc.inWinners(h) && !sc.inPending(h) })

	return append(effectiveD1, filter(sc.pendingOrder(), func(h chainhash.Hash) bool { return !sc.inWinners(h) })...)
}

// onChainBefore reports whether h's outputs are on the chain ending in B. A lost-block transaction
// that is also pending (the X case) counts as pending, not as mined.
func (sc *reorgScenario) onChainBefore(h chainhash.Hash) bool {
	return isConfirmedParent(h) || (sc.inLost(h) && !sc.inPending(h))
}

// onChainAfter reports whether h's outputs are on the chain ending in Fn.
func (sc *reorgScenario) onChainAfter(h chainhash.Hash) bool {
	return isConfirmedParent(h) || sc.inWinners(h)
}

// scenarioCoinbase is a coinbase transaction unique to tag.
func scenarioCoinbase(t testing.TB, height uint32, tag uint64) *bt.Tx {
	t.Helper()

	var script [13]byte
	script[0] = 0x04
	binary.LittleEndian.PutUint32(script[1:], height)
	binary.LittleEndian.PutUint64(script[5:], tag)

	in := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes(script[:])}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{in}
	tx.Outputs = []*bt.Output{{Satoshis: 50_0000_0000, LockingScript: bscript.NewFromBytes([]byte{0x51})}}

	return tx
}

// markSpyStore records the longest-chain marks a reorg makes, then passes them to the real store.
// With countOnly it only counts them and passes nothing on.
type markSpyStore struct {
	utxo.Store

	countOnly bool

	mu     sync.Mutex
	marks  map[bool][]chainhash.Hash
	counts map[bool]int
}

func (s *markSpyStore) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	s.mu.Lock()
	if s.marks == nil {
		s.marks = make(map[bool][]chainhash.Hash)
		s.counts = make(map[bool]int)
	}

	s.counts[onLongestChain] += len(txHashes)

	if !s.countOnly {
		s.marks[onLongestChain] = append(s.marks[onLongestChain], txHashes...)
	}
	s.mu.Unlock()

	if s.countOnly {
		return nil
	}

	return s.Store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

// markCount is how many hashes were marked with onLongestChain so far.
func (s *markSpyStore) markCount(onLongestChain bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counts[onLongestChain]
}

// marked returns every hash marked with onLongestChain so far.
func (s *markSpyStore) marked(onLongestChain bool) []chainhash.Hash {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]chainhash.Hash(nil), s.marks[onLongestChain]...)
}
