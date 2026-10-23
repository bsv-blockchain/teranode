package utxoset

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

// SpendAndCreateMulti writes the NET effect of a list of transactions.
//
// The list is ordered parents first, and a block's transactions mostly spend each other: on
// mainnet above the checkpoint nearly every input of a chained block names a parent in the same
// block. Written one transaction at a time, every such output is inserted into the UTXO table,
// then deleted from it by its child moments later, and the delete copies it into the spend
// journal. The insert and the delete each touch the UTXO table's index, and that index is the
// working set that does not fit in memory. This method skips both: an output created and spent
// inside one chunk of the list never becomes a UTXO row. Its child's spend of it is written
// straight to the spend journal, which is the one row a spend leaves behind anyway.
//
// Above the checkpoint the list is written as whole families in self-contained chunks; see
// writeFamilies. Every chunk is one database transaction that spends its transactions' outside
// coins and stores the transactions together, so every commit leaves a true state: a transaction
// is stored if and only if its spends are made. There is nothing to give back after a failure
// and nothing to recover after a crash, and no writer needs to know whether another writer of the
// same transaction exists or is alive.
//
// At or below the checkpoint, a list shaped like quick validation's (a block carried from birth,
// outpoint-only spends) never reaches the chunk writer: nettedBelowGate sends it to netBelow,
// which has its own write.
//
// It does NOT net, and hands the list to the per-transaction default instead, for a list whose
// options the chunk writer does not model: create-only or spend-only, and frozen, conflicting or
// locked creates. Below the checkpoint, netBelow does model locked creates and writes the coins
// locked; above it, canNet sends a locked list to DefaultSpendAndCreateMulti.
//
// A transaction in the list that turns out to exist already, which the caller's pre-check makes
// a race with another writer, is reported MultiTxExisted. The chunk finds it under the
// transaction's lock, rolls back and runs again without it, so a child of it spends its outputs
// for real.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32,
	opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	list, err := utxo.PrepareSpendAndCreateMulti(txs, opts...)
	if err != nil {
		return nil, err
	}

	if len(txs) == 0 {
		return []utxo.SpendAndCreateMultiResult{}, nil
	}

	if below, err := s.nettedBelowGate(list.Options); below {
		if err != nil {
			return nil, err
		}

		return s.netBelow(ctx, txs, list, blockHeight)
	}

	if !s.canNet(list.Options) {
		return utxo.DefaultSpendAndCreateMulti(ctx, s, utxo.SpendAndCreateMultiWidthsFor(s.settings), txs, blockHeight, opts...)
	}

	return s.newNettedWrite(txs, list, blockHeight).writeFamilies(ctx)
}

// canNet reports whether the netted write models every option of the list.
func (s *Store) canNet(o *utxo.CreateOptions) bool {
	return !o.SpendOnly && !o.CreateOnly && !o.Frozen && !o.Conflicting && !o.Locked
}

// multiCreateChunkTxs bounds the transactions one chunk transaction writes. The create takes
// one advisory lock per transaction (lockTxids), held to commit, and Postgres keeps those in a
// shared table sized max_locks_per_transaction x max_connections, 6,400 slots at the defaults
// mainnet runs. A chunk cut by bytes alone put 30,000 locks in one transaction and failed with
// "out of shared memory". At this bound and multiConcurrency chunks in flight the list holds at
// most about 2,000. A variable so a test can force one transaction per chunk.
var multiCreateChunkTxs = 256

// multiCrash is the error a chunk fault hook returns to stop the write as a crash would: the
// process is gone, so no cleanup runs and the open chunk's database transaction is never
// committed. It is its own type, matched by identity, because the errors package matches its own
// errors by code: a sentinel built from it would also match every unrelated processing error.
type multiCrash struct{}

func (multiCrash) Error() string {
	return "[utxoset][SpendAndCreateMulti] simulated crash"
}

var errMultiCrash error = multiCrash{}

// multiCommitTimeout bounds a chunk's commit. The commit runs on a context detached from the
// caller's, so that a cancel landing while it is in flight cannot leave it committed on the
// server and failed on the client: detached, it gives a true answer unless the connection
// itself fails.
const multiCommitTimeout = time.Minute

// multiCreateCommitted is a test hook called with the transaction count of each chunk
// transaction that commits. nil in production.
var multiCreateCommitted func(txs int)

// multiCreateBeforeCommit is a test hook called just before each chunk transaction commits, with the context the commit will run under and the chunk's txids. nil in production.
var multiCreateBeforeCommit func(ctx context.Context, txids []chainhash.Hash)

// multiCreateAfterCommit is a test hook called after each chunk transaction commits and before
// its results are set. An error from it is returned as the commit's, which is the shape
// of a commit that the server made and the client did not hear about. nil in production.
var multiCreateAfterCommit func() error

// multiConcurrency is how many chunk transactions run at once. Each holds one pool connection
// and takes no second one, so at most half the pool, leaving the rest of the node its share,
// and at most 8, which with multiCreateChunkTxs bounds the advisory locks held at once.
func (s *Store) multiConcurrency() int {
	return min(max(int(s.pool.Config().MaxConns)/2, 1), 8)
}

// multiTx is one transaction of the list as the netted write tracks it.
type multiTx struct {
	pos     int
	tx      *bt.Tx
	txid    chainhash.Hash
	parents []int
	claims  []inputClaim
	spends  []*utxo.Spend
	result  utxo.SpendAndCreateMultiResult
}

func (it *multiTx) dead() bool {
	return it.result.Status == utxo.MultiTxFailed || it.result.Status == utxo.MultiTxParentFailed
}

func (it *multiTx) restoreClaims() {
	for i, in := range it.tx.Inputs {
		if in != nil && i < len(it.claims) {
			in.PreviousTxSatoshis = it.claims[i].satoshis
			in.PreviousTxScript = it.claims[i].script
		}
	}
}

type nettedWrite struct {
	s           *Store
	list        *utxo.SpendAndCreateMultiList
	blockHeight uint32
	items       []*multiTx
	// position is each transaction's index in items, by txid.
	position map[chainhash.Hash]int
}

// multiSlowListLog is the write time above which a list gets an info log line saying where the
// time went.
const multiSlowListLog = time.Second

func (s *Store) newNettedWrite(txs []*bt.Tx, list *utxo.SpendAndCreateMultiList, blockHeight uint32) *nettedWrite {
	position := make(map[chainhash.Hash]int, len(txs))
	for i := range txs {
		position[list.TxIDs[i]] = i
	}

	w := &nettedWrite{s: s, list: list, blockHeight: blockHeight, items: make([]*multiTx, len(txs)),
		position: position}

	for i, tx := range txs {
		it := &multiTx{pos: i, tx: tx, txid: list.TxIDs[i], parents: list.ParentsInList[i], claims: captureClaims(tx),
			spends: make([]*utxo.Spend, len(tx.Inputs))}

		w.items[i] = it
	}

	return w
}

func (w *nettedWrite) results() []utxo.SpendAndCreateMultiResult {
	out := make([]utxo.SpendAndCreateMultiResult, len(w.items))
	for i, it := range w.items {
		out[i] = it.result
	}

	return out
}

// nettedKey names one output by its parent's txid and its packed key.
type nettedKey [48]byte

// nettedSpender is the transaction in the list that spends a netted output, and which of its
// inputs does it. The input index goes into the journal row, so GetSpend names it.
type nettedSpender struct {
	txid []byte
	vin  int32
}

func newNettedKey(txid []byte, ukey [16]byte) nettedKey {
	var k nettedKey

	copy(k[:32], txid)
	copy(k[32:], ukey[:])

	return k
}

// nettedRows is a plan's spend-journal rows for the outputs the list spends.
type nettedRows struct {
	sats      []int64
	created   []int32
	spendable []int32
	flags     []int16
	mined     []int32
	blockIDs  []int32
	ukeys     [][16]byte
	txids     [][]byte
	spenders  [][]byte
	vins      []int32
	scripts   [][]byte
}

// takeNetted removes from the plan every UTXO a child in the list spends and returns the
// spend-journal row each becomes: the UTXO as the create would have written it, spent at the
// list's height by that child.
func (p *createPlan) takeNetted(spender map[nettedKey]nettedSpender) *nettedRows {
	j := &nettedRows{}
	keep := 0

	for c := range p.utxoTxids {
		by, ok := spender[newNettedKey(p.utxoTxids[c], p.utxoUkeys[c])]
		if !ok {
			p.utxoSats[keep] = p.utxoSats[c]
			p.utxoHeights[keep] = p.utxoHeights[c]
			p.utxoSpendable[keep] = p.utxoSpendable[c]
			p.utxoLeaves[keep] = p.utxoLeaves[c]
			p.utxoFlags[keep] = p.utxoFlags[c]
			p.utxoUkeys[keep] = p.utxoUkeys[c]
			p.utxoTxids[keep] = p.utxoTxids[c]
			p.utxoScripts[keep] = p.utxoScripts[c]
			p.utxoMined[keep] = p.utxoMined[c]
			p.utxoBlockIDs[keep] = p.utxoBlockIDs[c]
			keep++

			continue
		}

		j.sats = append(j.sats, p.utxoSats[c])
		j.created = append(j.created, p.utxoHeights[c])
		j.spendable = append(j.spendable, p.utxoSpendable[c])
		j.flags = append(j.flags, p.utxoFlags[c])
		j.mined = append(j.mined, p.utxoMined[c])
		j.blockIDs = append(j.blockIDs, p.utxoBlockIDs[c])
		j.ukeys = append(j.ukeys, p.utxoUkeys[c])
		j.txids = append(j.txids, p.utxoTxids[c])
		j.spenders = append(j.spenders, by.txid)
		j.vins = append(j.vins, by.vin)
		j.scripts = append(j.scripts, p.utxoScripts[c])
	}

	p.utxoSats = p.utxoSats[:keep]
	p.utxoHeights = p.utxoHeights[:keep]
	p.utxoSpendable = p.utxoSpendable[:keep]
	p.utxoLeaves = p.utxoLeaves[:keep]
	p.utxoFlags = p.utxoFlags[:keep]
	p.utxoUkeys = p.utxoUkeys[:keep]
	p.utxoTxids = p.utxoTxids[:keep]
	p.utxoScripts = p.utxoScripts[:keep]
	p.utxoMined = p.utxoMined[:keep]
	p.utxoBlockIDs = p.utxoBlockIDs[:keep]

	return j
}

// nettedJournalSQL writes the spend-journal rows of outputs created and spent inside one list.
// The columns are the ones spendJournalSQL copies off a deleted UTXO, with no hash_override: a
// UTXO this list creates has never been reassigned.
const nettedJournalSQL = `
INSERT INTO spend_journal (spent_height, spending_vin, satoshis, created_height, spendable_from, flags,
                           mined_height, block_id, ukey, txid, spending_txid, script)
SELECT $1::int, j.spending_vin, j.satoshis, j.created_height, j.spendable_from, j.flags,
       j.mined_height, j.block_id, j.ukey, j.txid, j.spending_txid, j.script
  FROM unnest($2::bigint[], $3::int[], $4::int[], $5::smallint[], $6::int[], $7::int[],
              $8::uuid[], $9::bytea[], $10::bytea[], $11::bytea[], $12::int[])
    AS j(satoshis, created_height, spendable_from, flags, mined_height, block_id,
         ukey, txid, spending_txid, script, spending_vin)`

func (w *nettedWrite) writeNettedJournal(ctx context.Context, dbTx pgx.Tx, j *nettedRows) error {
	if len(j.ukeys) == 0 {
		return nil
	}

	if _, err := dbTx.Exec(ctx, nettedJournalSQL, int32(w.blockHeight), //nolint:gosec // a height fits int32
		j.sats, j.created, j.spendable, j.flags, j.mined, j.blockIDs, j.ukeys, j.txids, j.spenders, j.scripts, j.vins); err != nil {
		return errors.NewStorageError("[utxoset][SpendAndCreateMulti] journal netted spends", err)
	}

	return nil
}
