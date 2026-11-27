package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"net/url"
	"slices"
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
	"github.com/bsv-blockchain/teranode/settings"
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

	// lanePositions runs the processor at utxostore_laneMigration shadow: each pending tx is loaded
	// with scenarioLanePosition(hash), the UTXO store answers a lane position read with the same
	// for every hash, and sc.laneKeeper records the lane bitmaps of the subtrees the processor builds.
	lanePositions bool

	// parallelThreshold is blockassembly_parallelSetIfNotExistsThreshold. Zero keeps the default of
	// 10_000, below which the remainder pass adds node by node and never reaches the wasInserted
	// compaction; a scenario of a few thousand transactions needs it lowered to run that branch.
	parallelThreshold int

	// The removeMap knobs put transactions in the processor's removeMap before the reorg, the way
	// Remove does for a transaction it cannot find yet. Each is a count; all four are disjoint:
	//   - removeMapPending: synthetic pending transactions that are not hits and have no child
	//   - removeMapHits: pending transactions the winners mined
	//   - removeMapLost: lost-block transactions that are also pending (X), beyond the first
	//     lostChildOfPending of them, which have a child that must stay valid
	//   - removeMapD1: lost-block transactions that are neither pending nor in the winners
	// The reorg consumes every entry: the transaction is absent from the assembly and the tx map
	// afterwards, and a removed lost-block transaction is not marked off the longest chain, because
	// that mark covers only what the assembly still holds.
	removeMapPending int
	removeMapHits    int
	removeMapLost    int
	removeMapD1      int

	// lostChildOfPending is how many lost-block transactions C (in B, not in the winners, not
	// pending) spend a lost-block transaction X that is also pending. They come first among the X
	// transactions, so lostAlsoPending must cover them. See the order of today's slow path in
	// TestReorgScenario_SlowPathPublishesLostChildAfterPendingParent.
	lostChildOfPending int

	// pendingSpendsLostChild makes the first of the pendingSpendsLost pending transactions spend the
	// last lost-block child C instead of its own raw D1 parent. It sits in the pending set ahead of
	// C's parent X, so the move-back must keep it behind C too.
	pendingSpendsLostChild bool

	// lostChildParentsInWinners puts the X transactions the lost-block children spend in the winning
	// blocks too (B, P and F), as when the winning miner mined X as well.
	lostChildParentsInWinners bool

	// lostChildChain makes the lost-block children a chain instead of one child per X: the first
	// spends X, and each following one spends the one before it (C spends X, D spends C).
	lostChildChain bool

	// lostChildTwoParents makes each lost-block child also spend a second X, X number
	// lostChildOfPending+j for child j, so lostAlsoPending must cover twice the children. The
	// second X comes later in the pending set, so the child follows the later of its two parents.
	lostChildTwoParents bool

	// coinbaseChildren is how many pending transactions spend B's coinbase. They are real
	// transactions with records in the UTXO store, and never hits. The slow path deletes them with
	// the coinbase (removeCoinbaseUtxos), out of the store, the assembly and the tx map, and leaves
	// their removeMap entries behind.
	coinbaseChildren int

	// duplicateInPending is how many pending transactions appear twice in the chained subtrees: a
	// second copy is added to the open subtree behind the tx map's back, as a failed path can leave
	// it. Half are transactions that survive the reorg, the others hits. The remainder pass keeps one
	// copy of a survivor, at its first position, and drops both copies of a hit.
	duplicateInPending int

	// emptyForwardFirst and emptyForwardLast add a forward block without subtrees before the
	// forwardBlocks blocks that hold the winners, and after them. An empty forward block only creates
	// its coinbase: it returns before resetSubtreeState, so it leaves the assembly and the subtree
	// size as they were.
	emptyForwardFirst bool
	emptyForwardLast  bool

	// conflictsInLost is how many lost-block transactions W won a conflict when B was applied: B's
	// subtree lists W as a conflicting node, W owns the output it shares with a loser L that is in no
	// set, and L is conflicting in the store. W is in B only, so it is in raw D1. Moving B back
	// reverses the conflict (W conflicting again, L restored) and evicts W through the removeMap.
	conflictsInLost int

	// conflictsInWinner is how many winning-block transactions W' are conflicting nodes of F that
	// displace a pending transaction L' (a real transaction in the UTXO store, pending, never a hit)
	// which has a pending child D' that the UTXO store has no record of, so only the assembly's own
	// parent links reach it. The forward step demotes L', drops D' from the remainder, and the marks
	// cover L'.
	conflictsInWinner int

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

	lostIDs    []scenarioID
	pendingIDs []scenarioID
	winnerIDs  []scenarioID
	rawD1Txs   []*bt.Tx

	// laneKeeper is set with cfg.lanePositions.
	laneKeeper *recordingLaneKeeper

	// cbChildTxs are the pending transactions that spend the lost block's coinbase, and
	// lostCoinbase is that coinbase.
	cbChildTxs   []*bt.Tx
	lostCoinbase *bt.Tx

	// conflictTxs are the real transactions of the winner-side conflicts, behind kindConflict ids:
	// loser L' at index 2j, winner W' at 2j+1. winnerPairs holds each with its parent. lostPairs are
	// the lost-side conflicts, W being raw D1 transaction conflictBase+j.
	conflictTxs []*bt.Tx
	winnerPairs []conflictPair
	lostPairs   []conflictPair

	// confirmedExtra are real transactions the scenario treats as on the chain before and after the
	// reorg: the parents of the conflicting spends.
	confirmedExtra map[chainhash.Hash]struct{}

	// removeIDs are the transactions put in the removeMap. droppedIDs are the ones the reorg must
	// leave out of the assembly and out of the mark-false set, whatever took them out.
	removeIDs []scenarioID
	dupIDs    []scenarioID

	// loserIDs are the displaced pending transactions of the winner-side conflicts: marked off the
	// longest chain although they leave the assembly.
	loserIDs   []scenarioID
	droppedIDs []scenarioID

	// extraParent is the second parent of a pending transaction that spends another scenario
	// transaction, with the output it spends.
	extraParent map[scenarioID]scenarioOutpoint

	setsOnce   sync.Once
	lostSet    map[chainhash.Hash]struct{}
	pendingSet map[chainhash.Hash]struct{}
	winnerSet  map[chainhash.Hash]struct{}
	droppedSet map[chainhash.Hash]struct{}
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
	kindCoinbaseChild
	kindConflict
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
	if real := sc.realTx(id); real != nil {
		return *real.TxIDChainHash()
	}

	return scenarioHash(sc.cfg.seed, uint64(id))
}

// conflictPair is a parent with two transactions that spend its output 0.
type conflictPair struct{ parent, loser, winner *bt.Tx }

// newConflictPair builds a parent that spends a confirmed parent, and a loser and a winner that
// both spend its output 0. tag keeps every output script unique.
func (sc *reorgScenario) newConflictPair(t testing.TB, tag uint64) conflictPair {
	t.Helper()

	build := func(prev chainhash.Hash, prevScript *bscript.Script, prevSatoshis, satoshis uint64, kind byte) *bt.Tx {
		in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: prevSatoshis, PreviousTxScript: prevScript, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
		require.NoError(t, in.PreviousTxIDAdd(&prev))

		var data [9]byte
		binary.LittleEndian.PutUint64(data[:8], sc.cfg.seed<<40^tag)
		data[8] = kind

		tx := bt.NewTx()
		tx.Inputs = []*bt.Input{in}
		tx.Outputs = []*bt.Output{{Satoshis: satoshis, LockingScript: bscript.NewFromBytes(append([]byte{0x6a, 0x09}, data[:]...))}}

		return tx
	}

	parent := build(scenarioHash(sc.cfg.seed^0xc0f11c7, tag), bscript.NewFromBytes([]byte{0x51}), 200_000, 100_000, 0)

	if sc.confirmedExtra == nil {
		sc.confirmedExtra = make(map[chainhash.Hash]struct{})
	}

	sc.confirmedExtra[*parent.TxIDChainHash()] = struct{}{}

	return conflictPair{parent: parent, loser: build(*parent.TxIDChainHash(), parent.Outputs[0].LockingScript, 100_000, 90_000, 1), winner: build(*parent.TxIDChainHash(), parent.Outputs[0].LockingScript, 100_000, 80_000, 2)}
}

// realTx is the real transaction behind id, or nil for a synthetic one.
func (sc *reorgScenario) realTx(id scenarioID) *bt.Tx {
	switch id.kind() {
	case kindRawD1:
		return sc.rawD1Txs[id.index()]
	case kindCoinbaseChild:
		return sc.cbChildTxs[id.index()]
	case kindConflict:
		return sc.conflictTxs[id.index()]
	default:
		return nil
	}
}

// tx is the node and inpoints of id.
func (sc *reorgScenario) tx(t testing.TB, id scenarioID) scenarioTx {
	if real := sc.realTx(id); real != nil {
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

	// A lost-block child of a pending X spends X's output instead of a confirmed parent.
	childBase := sc.cfg.lostAlsoPending + sc.cfg.pendingSpendsLost
	parents := []chainhash.Hash{parent}

	if i >= childBase && i < childBase+sc.cfg.lostChildOfPending {
		parents = nil
		for _, p := range sc.lostChildParents(i - childBase) {
			parents = append(parents, *sc.rawD1Txs[p].TxIDChainHash())
		}
	}

	// A lost-block conflict winner is the winner of a pair, which is also built here.
	conflictBase := childBase + sc.cfg.lostChildOfPending
	if i >= conflictBase && i < conflictBase+sc.cfg.conflictsInLost {
		pair := sc.newConflictPair(t, 1<<32|uint64(i))
		sc.lostPairs = append(sc.lostPairs, pair)

		return pair.winner
	}

	tx := bt.NewTx()

	for _, parent := range parents {
		in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 10_000, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{})}
		require.NoError(t, in.PreviousTxIDAdd(&parent))

		tx.Inputs = append(tx.Inputs, in)
	}

	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], sc.cfg.seed<<40^uint64(i))

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
	require.LessOrEqual(t, cfg.lostChildOfPending, cfg.lostAlsoPending, "each lost-block child spends one X")
	if cfg.lostChildTwoParents {
		require.LessOrEqual(t, 2*cfg.lostChildOfPending, cfg.lostAlsoPending, "each lost-block child spends two X")
		require.False(t, cfg.lostChildChain, "a child is a chain link or has two parents, not both")
	}
	require.LessOrEqual(t, cfg.lostAlsoPending+cfg.pendingSpendsLost+cfg.lostChildOfPending+cfg.conflictsInLost, rawD1Count, "room for the lost-block children and conflict winners")

	sc.rawD1Txs = make([]*bt.Tx, rawD1Count)
	for i := range rawD1Count {
		sc.rawD1Txs[i] = sc.rawD1Tx(t, i)
	}

	for j := range cfg.conflictsInWinner {
		pair := sc.newConflictPair(t, 2<<32|uint64(j))
		sc.winnerPairs = append(sc.winnerPairs, pair)
		sc.conflictTxs = append(sc.conflictTxs, pair.loser, pair.winner)
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
	sc.orderLostChildrenAfterParents()

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
	sc.placeCoinbaseChildren(t)
	sc.placeConflictLosers(t)

	// The hits: pending transactions the winners mined. Never a lostAlsoPending transaction, never a
	// child of a raw D1 transaction (the winners do not have its parent), and never a chain child
	// whose parent might not be a hit.
	hitCount := int(math.Round(float64(cfg.pending) * cfg.pendingInWinners))
	candidates := make([]uint32, 0, synthetic)

	for pos, id := range sc.pendingIDs {
		if id.kind() != kindPending {
			continue
		}

		if extra, ok := sc.extraParent[id]; ok && (extra.parent.kind() != kindPending || !cfg.hitsAtFront) {
			continue
		}

		candidates = append(candidates, uint32(pos))
	}

	if !cfg.hitsAtFront {
		rng.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] })
	}

	require.LessOrEqual(t, hitCount, len(candidates))

	sc.planRemoveMap(t, candidates[:hitCount])

	// The winners, shuffled: B's shared part, the hits and the winner-only transactions.
	sc.winnerIDs = make([]scenarioID, 0, lostShared+hitCount+cfg.winnerOnly)
	for i := range lostShared {
		sc.winnerIDs = append(sc.winnerIDs, kindLostShared|scenarioID(i))
	}

	for _, pos := range candidates[:hitCount] {
		sc.winnerIDs = append(sc.winnerIDs, sc.pendingIDs[pos])
	}

	if cfg.lostChildParentsInWinners {
		for x := range cfg.lostChildOfPending {
			sc.winnerIDs = append(sc.winnerIDs, kindRawD1|scenarioID(x))
		}
	}

	for i := range cfg.winnerOnly {
		sc.winnerIDs = append(sc.winnerIDs, kindWinnerOnly|scenarioID(i))
	}

	for j := range cfg.conflictsInWinner {
		sc.winnerIDs = append(sc.winnerIDs, kindConflict|scenarioID(2*j+1))
	}

	rng.Shuffle(len(sc.winnerIDs), func(i, j int) { sc.winnerIDs[i], sc.winnerIDs[j] = sc.winnerIDs[j], sc.winnerIDs[i] })

	sc.buildChain(t)
	sc.buildProcessor(t)

	return sc
}

// pickSpread returns n of ids, spread evenly through them, in order. It draws nothing from the
// scenario's random source, so a scenario without the knob that calls it is unchanged.
func pickSpread(ids []scenarioID, n int) []scenarioID {
	out := make([]scenarioID, 0, n)

	for j := range n {
		out = append(out, ids[(2*j+1)*len(ids)/(2*n)])
	}

	return out
}

// placeCoinbaseChildren replaces spread-out pending transactions that have no parent or child in the
// scenario with the coinbase children. They are never hits, which only take kindPending ones.
func (sc *reorgScenario) placeCoinbaseChildren(t testing.TB) {
	t.Helper()

	if sc.cfg.coinbaseChildren == 0 {
		return
	}

	hasChild := make(map[scenarioID]struct{}, len(sc.extraParent))
	for _, edge := range sc.extraParent {
		hasChild[edge.parent] = struct{}{}
	}

	var positions []scenarioID

	for pos, id := range sc.pendingIDs {
		_, isChild := sc.extraParent[id]
		_, isParent := hasChild[id]

		if id.kind() == kindPending && !isChild && !isParent {
			positions = append(positions, scenarioID(pos))
		}
	}

	require.LessOrEqual(t, sc.cfg.coinbaseChildren, len(positions))

	for j, pos := range pickSpread(positions, sc.cfg.coinbaseChildren) {
		id := kindCoinbaseChild | scenarioID(j)
		sc.pendingIDs[pos] = id
		sc.droppedIDs = append(sc.droppedIDs, id)
	}
}

// placeConflictLosers replaces spread-out pending transactions with the losers L' of the winner-side
// conflicts and makes the next free pending transaction after each one its child D'.
func (sc *reorgScenario) placeConflictLosers(t testing.TB) {
	t.Helper()

	n := sc.cfg.conflictsInWinner
	if n == 0 {
		return
	}

	hasChild := make(map[scenarioID]struct{}, len(sc.extraParent))
	for _, edge := range sc.extraParent {
		hasChild[edge.parent] = struct{}{}
	}

	free := func(id scenarioID) bool {
		_, isChild := sc.extraParent[id]
		_, isParent := hasChild[id]

		return id.kind() == kindPending && !isChild && !isParent
	}

	var positions []scenarioID

	for pos, id := range sc.pendingIDs {
		// leave room for the child behind it
		if free(id) && pos+1 < len(sc.pendingIDs) && free(sc.pendingIDs[pos+1]) {
			positions = append(positions, scenarioID(pos))
		}
	}

	require.LessOrEqual(t, n, len(positions))

	for j, pos := range pickSpread(positions, n) {
		loser := kindConflict | scenarioID(2*j)
		child := sc.pendingIDs[pos+1]

		sc.pendingIDs[pos] = loser
		sc.extraParent[child] = scenarioOutpoint{loser, 0}
		sc.droppedIDs = append(sc.droppedIDs, loser, child)
		sc.loserIDs = append(sc.loserIDs, loser)
	}
}

// planRemoveMap chooses the transactions the removeMap knobs name. hitPositions are the positions in
// the pending set of the pending transactions the winners mined.
func (sc *reorgScenario) planRemoveMap(t testing.TB, hitPositions []uint32) {
	t.Helper()

	cfg := sc.cfg
	if cfg.removeMapPending+cfg.removeMapHits+cfg.removeMapLost+cfg.removeMapD1+cfg.duplicateInPending+cfg.conflictsInLost == 0 {
		return
	}

	isHit := make(map[scenarioID]struct{}, len(hitPositions))
	for _, pos := range hitPositions {
		isHit[sc.pendingIDs[pos]] = struct{}{}
	}

	hasChild := make(map[scenarioID]struct{}, len(sc.extraParent))
	for _, edge := range sc.extraParent {
		hasChild[edge.parent] = struct{}{}
	}

	var plain, hits []scenarioID

	for _, id := range sc.pendingIDs {
		if id.kind() != kindPending {
			continue
		}

		if _, ok := isHit[id]; ok {
			hits = append(hits, id)

			continue
		}

		_, isChild := sc.extraParent[id]
		_, isParent := hasChild[id]

		if !isChild && !isParent {
			plain = append(plain, id)
		}
	}

	require.LessOrEqual(t, cfg.removeMapPending, len(plain), "removeMapPending needs pending transactions that are not hits and have no parent or child in the scenario")
	require.LessOrEqual(t, cfg.removeMapHits, len(hits))

	pending := pickSpread(plain, cfg.removeMapPending)
	sc.removeIDs = append(sc.removeIDs, pending...)
	sc.droppedIDs = append(sc.droppedIDs, pending...)
	sc.removeIDs = append(sc.removeIDs, pickSpread(hits, cfg.removeMapHits)...)

	if cfg.duplicateInPending > 0 {
		taken := sc.hashSet(sc.removeIDs)
		survivors := slices.DeleteFunc(slices.Clone(plain), func(id scenarioID) bool { _, ok := taken[sc.hash(id)]; return ok })
		keptHits := slices.DeleteFunc(slices.Clone(hits), func(id scenarioID) bool { _, ok := taken[sc.hash(id)]; return ok })

		nSurvivors := (cfg.duplicateInPending + 1) / 2
		require.LessOrEqual(t, nSurvivors, len(survivors))
		require.LessOrEqual(t, cfg.duplicateInPending-nSurvivors, len(keptHits))

		sc.dupIDs = append(pickSpread(survivors, nSurvivors), pickSpread(keptHits, cfg.duplicateInPending-nSurvivors)...)
	}

	// The X transactions a lost-block child spends come first, then the ones the removeMap takes.
	xFirst := cfg.lostChildOfPending
	require.LessOrEqual(t, xFirst+cfg.removeMapLost, cfg.lostAlsoPending, "removeMapLost needs lostAlsoPending transactions the lost-block children do not spend")

	for j := range cfg.removeMapLost {
		id := kindRawD1 | scenarioID(xFirst+j)
		sc.removeIDs = append(sc.removeIDs, id)
		sc.droppedIDs = append(sc.droppedIDs, id)
	}

	conflictBase := cfg.lostAlsoPending + cfg.pendingSpendsLost + cfg.lostChildOfPending
	for j := range cfg.conflictsInLost {
		sc.droppedIDs = append(sc.droppedIDs, kindRawD1|scenarioID(conflictBase+j))
	}

	d1Base := cfg.lostAlsoPending + cfg.pendingSpendsLost + cfg.lostChildOfPending + cfg.conflictsInLost
	require.LessOrEqual(t, d1Base+cfg.removeMapD1, len(sc.rawD1Txs), "removeMapD1 needs raw D1 transactions beyond the ones with a role")

	for j := range cfg.removeMapD1 {
		id := kindRawD1 | scenarioID(d1Base+j)
		sc.removeIDs = append(sc.removeIDs, id)
		sc.droppedIDs = append(sc.droppedIDs, id)
	}
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
		target := cfg.lostAlsoPending + j
		if cfg.pendingSpendsLostChild && j == 0 {
			require.Positive(t, cfg.lostChildOfPending, "pendingSpendsLostChild needs a lost-block child")
			target = cfg.lostAlsoPending + cfg.pendingSpendsLost + cfg.lostChildOfPending - 1
		}

		sc.extraParent[kindPending|scenarioID(child)] = scenarioOutpoint{kindRawD1 | scenarioID(target), 0}
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
	sc.lostCoinbase = sc.lost.CoinbaseTx
	require.NoError(t, sc.client.AddBlock(ctx, sc.lost, "", blockchainoptions.WithMinedSet(true)))

	parent := sc.genesis
	total := sc.cfg.forwardBlocks
	sc.forward = make([]*model.Block, 0, total+2)

	if sc.cfg.emptyForwardFirst {
		total++
	}

	if sc.cfg.emptyForwardLast {
		total++
	}

	withWinners := 0

	for i := range total {
		header := childHeader(parent, nonce+uint32(i)+1)

		var block *model.Block

		if (sc.cfg.emptyForwardFirst && i == 0) || (sc.cfg.emptyForwardLast && i == total-1) {
			block = &model.Block{
				Height:           uint32(i + 1), //nolint:gosec // small index
				Header:           header,
				CoinbaseTx:       scenarioCoinbase(t, uint32(i+1), uint64(header.Nonce), 1), //nolint:gosec // small index
				TransactionCount: 1,
				SizeInBytes:      200,
				Subtrees:         []*chainhash.Hash{},
			}
		} else {
			lo, hi := withWinners*len(sc.winnerIDs)/sc.cfg.forwardBlocks, (withWinners+1)*len(sc.winnerIDs)/sc.cfg.forwardBlocks
			block = sc.storeBlock(t, uint32(i+1), header, sc.winnerIDs[lo:hi], false) //nolint:gosec // small index
			withWinners++
		}

		require.NoError(t, sc.client.AddBlock(ctx, block, "", blockchainoptions.WithMinedSet(true)))
		sc.forward = append(sc.forward, block)
		parent = block.Header
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///reorgscenario")
	require.NoError(t, err)

	sqlStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	sc.utxo = &markSpyStore{Store: sqlStore, countOnly: sc.cfg.skipStoreMarks}

	conflictBase := sc.cfg.lostAlsoPending + sc.cfg.pendingSpendsLost + sc.cfg.lostChildOfPending

	for i, tx := range sc.rawD1Txs {
		if sc.cfg.skipStoreMarks {
			break
		}

		if i >= conflictBase && i < conflictBase+sc.cfg.conflictsInLost {
			continue // created below, as a conflicting winner
		}

		_, err = sqlStore.Create(ctx, tx, sc.lost.Height)
		require.NoError(t, err)
	}

	// Winner-side conflicts: the loser owns the parent's output and is pending, the winner waits in F.
	for _, pair := range sc.winnerPairs {
		_, _, err = sqlStore.SpendAndCreate(ctx, pair.parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = sqlStore.SpendAndCreate(ctx, pair.loser, 2)
		require.NoError(t, err)

		_, _, err = sqlStore.SpendAndCreate(ctx, pair.winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)
	}

	// Lost-side conflicts, in the state B being applied left them: the winner owns the output and the
	// loser is conflicting.
	winners := make([]chainhash.Hash, 0, len(sc.lostPairs))

	for _, pair := range sc.lostPairs {
		_, _, err = sqlStore.SpendAndCreate(ctx, pair.parent, 1, utxo.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = sqlStore.SpendAndCreate(ctx, pair.loser, 2)
		require.NoError(t, err)

		_, _, err = sqlStore.SpendAndCreate(ctx, pair.winner, 2, utxo.WithCreateOnly(), utxo.WithConflicting(true))
		require.NoError(t, err)

		winners = append(winners, *pair.winner.TxIDChainHash())
	}

	if len(winners) > 0 {
		_, _, err = utxo.ProcessConflicting(ctx, sqlStore, sc.lost.Height, *sc.lost.Hash(), winners, nil, utxo.NoAncestryGuard)
		require.NoError(t, err)
	}

	_, _, err = sqlStore.SpendAndCreate(ctx, sc.lost.CoinbaseTx, sc.lost.Height, utxo.WithCreateOnly())
	require.NoError(t, err)

	// The coinbase children spend one coinbase output each, at the height the output matures.
	spendHeight := sc.lost.Height + uint32(tSettings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec // small chain constant
	sc.cbChildTxs = make([]*bt.Tx, sc.cfg.coinbaseChildren)

	for i := range sc.cbChildTxs {
		child := bt.NewTx()
		require.NoError(t, child.From(sc.lost.CoinbaseTx.TxIDChainHash().String(), uint32(i), sc.lost.CoinbaseTx.Outputs[i].LockingScript.String(), sc.lost.CoinbaseTx.Outputs[i].Satoshis)) //nolint:gosec // small index
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
		child.Outputs = []*bt.Output{{Satoshis: 49_0000_0000, LockingScript: bscript.NewFromBytes([]byte{0x6a, 0x08, byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0})}}

		_, _, err = sqlStore.SpendAndCreate(ctx, child, spendHeight)
		require.NoError(t, err)

		sc.cbChildTxs[i] = child
	}
}

// isConflictNode reports whether the block holding id lists it as a conflicting node: the winner of a
// conflict, in the lost block or in a winning block.
func (sc *reorgScenario) isConflictNode(id scenarioID) bool {
	conflictBase := sc.cfg.lostAlsoPending + sc.cfg.pendingSpendsLost + sc.cfg.lostChildOfPending

	switch id.kind() {
	case kindConflict:
		return id.index()%2 == 1
	case kindRawD1:
		return id.index() >= conflictBase && id.index() < conflictBase+sc.cfg.conflictsInLost
	default:
		return false
	}
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
		CoinbaseTx:       scenarioCoinbase(t, height, uint64(header.Nonce), map[bool]int{true: sc.cfg.coinbaseChildren}[meta]),
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

			if sc.isConflictNode(id) {
				require.NoError(t, st.AddConflictingNode(tx.node.Hash))
			}

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

	if sc.cfg.parallelThreshold > 0 {
		tSettings.BlockAssembly.ParallelSetIfNotExistsThreshold = sc.cfg.parallelThreshold
	}

	var opts []Options

	if sc.cfg.lanePositions {
		tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		sc.utxo.Store = &scenarioLaneStore{Store: sc.utxo.Store}
		sc.laneKeeper = newRecordingLaneKeeper()
		opts = append(opts, WithLaneBitmapsKeeper(sc.laneKeeper))
	}

	if sc.cfg.diskTxMap {
		opts = append(opts, WithTxMapDirs([]string{t.TempDir()}))
	}

	if sc.cfg.storedSet != nil {
		opts = append(opts, WithStoredSet(sc.cfg.storedSet))
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

	t.Cleanup(func() {
		stp.Stop(context.Background())
		close(newSubtreeChan)
		<-listenerDone
	})

	stp.InitCurrentBlockHeader(sc.lost.Header)

	const batchSize = 1 << 16

	for start := 0; start < len(sc.pendingIDs); start += batchSize {
		ids := sc.pendingIDs[start:min(start+batchSize, len(sc.pendingIDs))]
		txs := make([]scenarioTx, len(ids))
		batch := make([]*utxo.UnminedTransaction, len(ids))

		for i, id := range ids {
			txs[i] = sc.tx(t, id)
			batch[i] = &utxo.UnminedTransaction{Node: &txs[i].node, TxInpoints: &txs[i].inpoints, CreatedAt: start + i}

			if sc.cfg.lanePositions {
				batch[i].LanePosition = scenarioLanePosition(txs[i].node.Hash)
			}
		}

		require.NoError(t, stp.AddNodesDirectly(batch, true))
	}

	for _, id := range sc.removeIDs {
		require.NoError(t, stp.removeMap.Put(sc.hash(id), 1))
	}

	if len(sc.dupIDs) > 0 {
		open := stp.currentSubtree.Load()
		require.Greater(t, open.Size()-open.Length(), len(sc.dupIDs), "the open subtree has room for the duplicates")

		for _, id := range sc.dupIDs {
			require.NoError(t, open.AddSubtreeNode(sc.tx(t, id).node))
		}
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
		sc.droppedSet = sc.hashSet(sc.droppedIDs)
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

func (sc *reorgScenario) dropped(h chainhash.Hash) bool {
	sc.ensureSets()
	_, ok := sc.droppedSet[h]

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
// the pending transactions) in B's order, then P minus F in P's order, without the transactions the
// removeMap or the coinbase and conflict cleanup took out.
func (sc *reorgScenario) expectedOrderAfter() []chainhash.Hash {
	children, parents := sc.heldLostChildren()
	isHeld := sc.hashSet(nil)

	for _, c := range children {
		isHeld[c] = struct{}{}
	}

	effectiveD1 := filter(sc.lostOrder(), func(h chainhash.Hash) bool {
		_, held := isHeld[h]

		return !held && !sc.inWinners(h) && !sc.inPending(h) && !sc.dropped(h)
	})
	pending := filter(sc.pendingOrder(), func(h chainhash.Hash) bool { return !sc.inWinners(h) && !sc.dropped(h) })

	if len(children) == 0 {
		return append(effectiveD1, pending...)
	}

	// A held-back child follows the last of its parents in the order built so far, with the pending
	// transactions in their own order; children that become ready together keep B's order. A pending
	// transaction that spends a held-back child waits for it the same way. A transaction a winning
	// block mined takes its place in this walk but is not in the result.
	isChild := make(map[chainhash.Hash]struct{}, len(children))
	for _, c := range children {
		isChild[c] = struct{}{}
	}

	waiting := slices.Clone(children)
	pendingParents := make(map[chainhash.Hash][]chainhash.Hash)

	for id, e := range sc.extraParent {
		if _, ok := isChild[sc.hash(e.parent)]; ok {
			pendingParents[sc.hash(id)] = []chainhash.Hash{sc.hash(e.parent)}
		}
	}

	order := effectiveD1
	emitted := make(map[chainhash.Hash]struct{}, len(pending))
	done := make(map[chainhash.Hash]struct{}, len(children))

	ready := func(h chainhash.Hash) bool {
		ps := parents[h]
		if ps == nil {
			ps = pendingParents[h]
		}

		for _, p := range ps {
			if _, ok := emitted[p]; !ok {
				return false
			}
		}

		return true
	}

	emit := func(h chainhash.Hash) {
		emitted[h] = struct{}{}

		if !sc.inWinners(h) {
			order = append(order, h)
		}
	}

	for _, h := range filter(sc.pendingOrder(), func(h chainhash.Hash) bool { return !sc.dropped(h) }) {
		if _, spendsChild := pendingParents[h]; spendsChild && !ready(h) {
			waiting = append(waiting, h)

			continue
		}

		emit(h)

		for released := true; released; {
			released = false

			for _, w := range waiting {
				if _, already := done[w]; already || !ready(w) {
					continue
				}

				emit(w)
				done[w] = struct{}{}
				released = true
			}
		}
	}

	return order
}

// lostChildParents is the raw D1 indexes lost-block child j spends.
func (sc *reorgScenario) lostChildParents(j int) []int {
	cfg := sc.cfg
	childBase := cfg.lostAlsoPending + cfg.pendingSpendsLost

	switch {
	case cfg.lostChildChain && j > 0:
		return []int{childBase + j - 1}
	case cfg.lostChildTwoParents:
		return []int{j, cfg.lostChildOfPending + j}
	default:
		return []int{j}
	}
}

// orderLostChildrenAfterParents puts the X transactions and the lost-block children in B in an order
// that has every parent before its child, as a block does; the shuffle does not. It reuses the
// positions those transactions already hold, so the rest of B keeps its order.
func (sc *reorgScenario) orderLostChildrenAfterParents() {
	n := sc.cfg.lostChildOfPending
	if n == 0 {
		return
	}

	childBase := sc.cfg.lostAlsoPending + sc.cfg.pendingSpendsLost
	inOrder := make([]scenarioID, 0, sc.cfg.lostAlsoPending+n)

	for x := range sc.cfg.lostAlsoPending {
		inOrder = append(inOrder, kindRawD1|scenarioID(x))
	}

	for j := range n {
		inOrder = append(inOrder, kindRawD1|scenarioID(childBase+j))
	}

	role := make(map[scenarioID]struct{}, len(inOrder))
	for _, id := range inOrder {
		role[id] = struct{}{}
	}

	next := 0

	for i, id := range sc.lostIDs {
		if _, ok := role[id]; ok {
			sc.lostIDs[i] = inOrder[next]
			next++
		}
	}
}

// heldLostChildren is the lost-block children the move-back holds back, in B's order, each with the
// raw D1 transactions it spends that are not on chain after the move-back (the X and, in a chain,
// the children before it).
func (sc *reorgScenario) heldLostChildren() (children []chainhash.Hash, parents map[chainhash.Hash][]chainhash.Hash) {
	parents = make(map[chainhash.Hash][]chainhash.Hash)
	childBase := sc.cfg.lostAlsoPending + sc.cfg.pendingSpendsLost

	for j := range sc.cfg.lostChildOfPending {
		c := sc.hash(kindRawD1 | scenarioID(childBase+j))
		children = append(children, c)

		for _, p := range sc.lostChildParents(j) {
			parents[c] = append(parents[c], sc.hash(kindRawD1|scenarioID(p)))
		}
	}

	return children, parents
}

// expectedMarkFalse is what the reorg marks off the longest chain: raw D1, less the transactions that
// left the assembly before the marks, which cover only what the assembly still holds.
func (sc *reorgScenario) expectedMarkFalse() []chainhash.Hash {
	return append(filter(sc.rawD1(), func(h chainhash.Hash) bool { return !sc.dropped(h) }), sc.hashes(sc.loserIDs)...)
}

// expectedRemoveMapLength is what is left in the removeMap after the reorg: nothing the scenario
// put there, but the entries removeCoinbaseUtxos made for the coinbase children, which are no longer
// in any subtree for the remainder pass to consume them from (design 5, gate on removeMap and what
// removeCoinbaseUtxos adds).
func (sc *reorgScenario) expectedRemoveMapLength() int { return sc.cfg.coinbaseChildren }

// onChainBefore reports whether h's outputs are on the chain ending in B. A lost-block transaction
// that is also pending (the X case) counts as pending, not as mined.
func (sc *reorgScenario) onChainBefore(h chainhash.Hash) bool {
	_, extra := sc.confirmedExtra[h]

	return isConfirmedParent(h) || extra || (sc.inLost(h) && !sc.inPending(h)) || (sc.lostCoinbase != nil && *sc.lostCoinbase.TxIDChainHash() == h)
}

// onChainAfter reports whether h's outputs are on the chain ending in Fn.
func (sc *reorgScenario) onChainAfter(h chainhash.Hash) bool {
	_, extra := sc.confirmedExtra[h]

	return isConfirmedParent(h) || extra || sc.inWinners(h)
}

// scenarioCoinbase is a coinbase transaction unique to tag, with outputs outputs (at least one).
func scenarioCoinbase(t testing.TB, height uint32, tag uint64, outputs int) *bt.Tx {
	t.Helper()

	var script [13]byte
	script[0] = 0x04
	binary.LittleEndian.PutUint32(script[1:], height)
	binary.LittleEndian.PutUint64(script[5:], tag)

	in := &bt.Input{PreviousTxOutIndex: 0xffffffff, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes(script[:])}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{}))

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{in}
	for range max(outputs, 1) {
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 50_0000_0000, LockingScript: bscript.NewFromBytes([]byte{0x51})})
	}

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
