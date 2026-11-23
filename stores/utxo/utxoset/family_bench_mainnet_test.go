package utxoset

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestFamilyChunksOnMainnetBlocks times the family-chunk writer of SpendAndCreateMulti, blocks per
// second, on real mainnet blocks above the checkpoint. It is a benchmark and does nothing unless
// UTXOSET_FAMILY_BENCH_BLOCKS names a directory of raw block files (<height>.bin, wire format).
// Knobs:
//
//	UTXOSET_FAMILY_BENCH_BLOCKS  directory of <height>.bin files; the height is the file name
//	UTXOSET_FAMILY_BENCH_ONLY    comma-separated heights to run, default every file
//	UTXOSET_FAMILY_BENCH_REPS    measured repetitions per block, default 5
//	UTXOSET_FAMILY_BENCH_OUT     file the result table is also written to
//
// Run it against a real PostgreSQL (UTXOSET_TEST_DSN), as `rtk proxy go test -run
// TestFamilyChunksOnMainnetBlocks -v -timeout 2h ./stores/utxo/utxoset`, so the filter does not
// eat the table.
//
// What it does. It parses the block, skips the coinbase, seeds one coin for every outpoint whose
// parent is not in the block, with an arbitrary value and script, and sets each input's claimed
// value and script to match: the seeded coin for an outside input, the parent's real output for
// an in-block one. It then drops the transactions the store already holds, as the caller does,
// and calls SpendAndCreateMulti exactly as services/subtreevalidation's writeList does, on a
// freshly seeded store each repetition.
//
// What it measures: statement count, commit count and wall time on a small, freshly seeded
// store whose coin index sits in memory. What it does NOT measure: cold coin-index reads on a
// large store, which is where a mainnet node spends its time. Wall time here is dominated by
// statement and commit overhead.
func TestFamilyChunksOnMainnetBlocks(t *testing.T) {
	dir := os.Getenv("UTXOSET_FAMILY_BENCH_BLOCKS")
	if dir == "" {
		t.Skip("set UTXOSET_FAMILY_BENCH_BLOCKS to a directory of <height>.bin raw blocks to run this benchmark")
	}

	if os.Getenv("UTXOSET_TEST_DSN") == "" {
		t.Skip("the benchmark needs a real PostgreSQL: set UTXOSET_TEST_DSN")
	}

	reps := 5
	if v := os.Getenv("UTXOSET_FAMILY_BENCH_REPS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)

		reps = n
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.bin"))
	require.NoError(t, err)
	sort.Strings(files)

	only := map[string]bool{}

	if v := os.Getenv("UTXOSET_FAMILY_BENCH_ONLY"); v != "" {
		for _, h := range strings.Split(v, ",") {
			only[strings.TrimSpace(h)] = true
		}
	}

	tracer := &stmtTracer{}

	poolTracer = tracer

	t.Cleanup(func() { poolTracer = nil })

	var out strings.Builder

	out.WriteString("height\ttxs\tfamilies\tlargest\tchunks\tcut_edges\twriter\tmedian_s\tblocks_per_s\tmin_s\tmax_s\tstatements\tcommits\tbegin\tlock\tprobe\tspend\tspend_stmts\tspend_rows\tcreate\tcreate_span\tcreate_stmts\tjournal\tjournal_stmts\tcommit_t\tother\tcreated\tfailed\tparentfailed\texisted\n")

	for _, f := range files {
		height := strings.TrimSuffix(filepath.Base(f), ".bin")
		if len(only) > 0 && !only[height] {
			continue
		}

		h, err := strconv.Atoi(height)
		require.NoError(t, err)
		require.Greater(t, h, testCheckpointHeight, "the benchmark is for blocks above the checkpoint")

		raw, err := os.ReadFile(f)
		require.NoError(t, err)

		blk := parseBenchBlock(t, raw, uint32(h)) //nolint:gosec // a height fits

		t.Logf("block %d: %d transactions, %d outside coins, %d families (largest %d), %d dependency levels (widest %d), %d chunks, %d cut edges, %d chunks on the longest dependency chain",
			h, len(blk.txs), len(blk.seed.txids), blk.families, blk.largest, blk.levels, blk.widest, blk.chunks, blk.cutEdges, blk.critical)

		type sample struct {
			wall time.Duration
			st   stmtSnapshot
			res  [4]int
		}

		runs := map[string][]sample{}

		for rep := -1; rep < reps; rep++ { // rep -1 is a warm-up and is dropped
			name := "family"

			t.Run(fmt.Sprintf("%d/%s/rep%d", h, name, rep), func(t *testing.T) {
				s, ctx := newTestStoreWith(t, nil)
				seedBenchCoins(t, s, ctx, blk)
				blk.applyClaims()

				// The caller's pre-check: drop what the store already holds. A fresh store
				// holds nothing, so every transaction stays.
				txs := blk.precheck(t, s, ctx)
				txids := make([]chainhash.Hash, len(txs))

				for i, tx := range txs {
					txids[i] = *tx.TxIDChainHash()
				}

				multiFamilyDone = func(st *familyStats) {
					t.Logf("family write: %d chunks, %d rounds, plan %s, lock %s, probe %s, spend %s, create %s, commit %s, waiting on parents %s (summed over chunks)",
						st.chunks, st.rounds.Load(), time.Duration(st.planNs.Load()).Round(time.Millisecond),
						time.Duration(st.lockNs.Load()).Round(time.Millisecond), time.Duration(st.probeNs.Load()).Round(time.Millisecond),
						time.Duration(st.spendNs.Load()).Round(time.Millisecond), time.Duration(st.createNs.Load()).Round(time.Millisecond),
						time.Duration(st.commitNs.Load()).Round(time.Millisecond), time.Duration(st.waitingNs.Load()).Round(time.Millisecond))
				}

				t.Cleanup(func() { multiFamilyDone = nil })

				tracer.begin()
				t0 := time.Now()

				results, err := s.SpendAndCreateMulti(ctx, txs, blk.height, utxo.WithTXIDs(txids), utxo.WithIgnoreLocked(true))

				wall := time.Since(t0)
				snap := tracer.end()

				require.NoError(t, err)
				require.Len(t, results, len(txs))

				var res [4]int

				for _, r := range results {
					switch r.Status {
					case utxo.MultiTxCreated:
						res[0]++
					case utxo.MultiTxFailed:
						res[1]++
					case utxo.MultiTxParentFailed:
						res[2]++
					case utxo.MultiTxExisted:
						res[3]++
					}
				}

				if rep >= 0 {
					runs[name] = append(runs[name], sample{wall: wall, st: snap, res: res})
				}
			})
		}

		for _, name := range []string{"family"} {
			ss := runs[name]
			if len(ss) == 0 {
				continue
			}

			sort.Slice(ss, func(i, j int) bool { return ss[i].wall < ss[j].wall })

			med := ss[len(ss)/2]
			sec := med.wall.Seconds()

			line := fmt.Sprintf("%d\t%d\t%d\t%d\t%d\t%d\t%s\t%.3f\t%.3f\t%.3f\t%.3f\t%d\t%d\t%d\t%.3f\t%.3f\t%.3f\t%d\t%d\t%.3f\t%.3f\t%d\t%.3f\t%d\t%.3f\t%.3f\t%d\t%d\t%d\t%d\n",
				h, len(blk.txs), blk.families, blk.largest, blk.chunks, blk.cutEdges, name, sec, 1/sec,
				ss[0].wall.Seconds(), ss[len(ss)-1].wall.Seconds(),
				med.st.statements, med.st.count["commit"], med.st.count["begin"],
				med.st.dur["lock"].Seconds(), med.st.dur["probe"].Seconds(), med.st.dur["spend"].Seconds(), med.st.count["spend"], med.st.rows["spend"],
				med.st.dur["create"].Seconds(), med.st.last["create"].Sub(med.st.first["create"]).Seconds(), med.st.count["create"], med.st.dur["journal"].Seconds(), med.st.count["journal"],
				med.st.dur["commit"].Seconds(), med.st.dur["other"].Seconds(),
				med.res[0], med.res[1], med.res[2], med.res[3])
			out.WriteString(line)
		}
	}

	t.Log("\n" + out.String())

	if path := os.Getenv("UTXOSET_FAMILY_BENCH_OUT"); path != "" {
		require.NoError(t, os.WriteFile(path, []byte(out.String()), 0o600))
	}
}

// benchBlock is a parsed block and what the benchmark seeds for it.
type benchBlock struct {
	height uint32
	txs    []*bt.Tx // coinbase skipped
	// claims[i][vin] is what input vin of tx i claims about the coin it spends.
	claims [][]benchClaim
	seed   benchSeed

	families, largest, chunks, cutEdges, critical int
	// levels is the depth of the dependency graph, widest the most transactions on one level.
	levels, widest int
}

type benchClaim struct {
	sats   uint64
	script *bscript.Script
}

// benchSeed is the coins to insert: one per outpoint whose parent is not in the block.
type benchSeed struct {
	txids [][]byte
	vouts []uint32
	sats  []int64
}

var benchScript = func() *bscript.Script {
	s, err := bscript.NewFromHexString("76a914" + strings.Repeat("ab", 20) + "88ac")
	if err != nil {
		panic(err)
	}

	return s
}()

// readVarInt reads a Bitcoin varint.
func readVarInt(b []byte) (uint64, int) {
	switch b[0] {
	case 0xfd:
		return uint64(b[1]) | uint64(b[2])<<8, 3
	case 0xfe:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16 | uint64(b[4])<<24, 5
	case 0xff:
		var v uint64
		for i := 0; i < 8; i++ {
			v |= uint64(b[1+i]) << (8 * i)
		}

		return v, 9
	default:
		return uint64(b[0]), 1
	}
}

// parseBenchBlock parses a raw wire-format block, skips the coinbase, and works out the seed and
// the claims. In-block parents must be earlier in the block, as in every valid block.
func parseBenchBlock(t *testing.T, raw []byte, height uint32) *benchBlock {
	t.Helper()

	require.Greater(t, len(raw), 81)

	n, used := readVarInt(raw[80:])
	off := 80 + used

	all := make([]*bt.Tx, 0, n)

	for i := uint64(0); i < n; i++ {
		tx, size, err := bt.NewTxFromStream(raw[off:])
		require.NoError(t, err, "transaction %d", i)

		all = append(all, tx)
		off += size
	}

	require.Equal(t, len(raw), off, "trailing bytes after the last transaction")

	blk := &benchBlock{height: height, txs: all[1:]}

	position := make(map[chainhash.Hash]int, len(blk.txs))
	for i, tx := range blk.txs {
		position[*tx.TxIDChainHash()] = i
	}

	blk.claims = make([][]benchClaim, len(blk.txs))
	parents := make([][]int, len(blk.txs))
	sizes := make([]int, len(blk.txs))

	for i, tx := range blk.txs {
		sizes[i] = tx.Size()

		var outSum uint64
		for _, o := range tx.Outputs {
			outSum += o.Satoshis
		}

		blk.claims[i] = make([]benchClaim, len(tx.Inputs))

		for vin, in := range tx.Inputs {
			parent := *in.PreviousTxIDChainHash()

			if p, ok := position[parent]; ok {
				require.Less(t, p, i, "block is not parents-first")

				out := blk.txs[p].Outputs[in.PreviousTxOutIndex]
				blk.claims[i][vin] = benchClaim{sats: out.Satoshis, script: out.LockingScript}

				if !slices.Contains(parents[i], p) {
					parents[i] = append(parents[i], p)
				}

				continue
			}

			// An outside coin: any value at least what the transaction pays out, so its fee is
			// never negative, and one script.
			sats := max(outSum, 1_000)
			blk.claims[i][vin] = benchClaim{sats: sats, script: benchScript}

			blk.seed.txids = append(blk.seed.txids, parent[:])
			blk.seed.vouts = append(blk.seed.vouts, in.PreviousTxOutIndex)
			blk.seed.sats = append(blk.seed.sats, int64(sats)) //nolint:gosec // satoshis fit int64
		}
	}

	level := make([]int, len(parents))
	width := map[int]int{}

	for i, ps := range parents {
		for _, p := range ps {
			level[i] = max(level[i], level[p]+1)
		}

		width[level[i]]++
		blk.levels = max(blk.levels, level[i]+1)
		blk.widest = max(blk.widest, width[level[i]])
	}

	plan := planFamilyChunks(parents, sizes, multiCreateChunkTxs, spendAndCreateBatchByteBudget)
	blk.families, blk.largest, blk.chunks, blk.cutEdges = plan.families, plan.largest, len(plan.chunks), plan.cutEdges

	if ranks, _ := plan.ranks(); len(ranks) > 0 {
		blk.critical = slices.Max(ranks)
	}

	return blk
}

// applyClaims sets every input's claimed value and script, which a write overwrites with the
// store's own answer.
func (b *benchBlock) applyClaims() {
	for i, tx := range b.txs {
		for vin, in := range tx.Inputs {
			in.PreviousTxSatoshis = b.claims[i][vin].sats
			in.PreviousTxScript = b.claims[i][vin].script
		}
	}
}

// seedBenchCoins inserts the block's outside coins, mined well below the block.
func seedBenchCoins(t *testing.T, s *Store, ctx context.Context, b *benchBlock) {
	t.Helper()

	const batch = 50_000

	for start := 0; start < len(b.seed.txids); start += batch {
		end := min(start+batch, len(b.seed.txids))
		n := end - start

		sats := make([]int64, n)
		created := make([]int32, n)
		leaves := make([]int16, n)
		flags := make([]int16, n)
		ukeys := make([][16]byte, n)
		scripts := make([][]byte, n)
		mined := make([]int32, n)
		blocks := make([]int32, n)

		for i := 0; i < n; i++ {
			txid := b.seed.txids[start+i]
			sats[i] = b.seed.sats[start+i]
			created[i] = int32(b.height) - 100 //nolint:gosec // a height fits
			mined[i] = created[i]
			blocks[i] = 1
			leaves[i] = LeafFor(txid)
			ukeys[i] = Pack(txid, b.seed.vouts[start+i])
			scripts[i] = *benchScript
		}

		_, err := s.pool.Exec(ctx, `
INSERT INTO utxo (satoshis, created_height, spendable_from, mined_height, block_id, leaf, flags, ukey, txid, script)
SELECT o.satoshis, o.created_height, o.created_height, o.mined_height, o.block_id, o.leaf, o.flags, o.ukey, o.txid, o.script
  FROM unnest($1::bigint[], $2::int[], $3::int[], $4::int[], $5::smallint[], $6::smallint[], $7::uuid[], $8::bytea[], $9::bytea[])
    AS o(satoshis, created_height, mined_height, block_id, leaf, flags, ukey, txid, script)`,
			sats, created, mined, blocks, leaves, flags, ukeys, b.seed.txids[start:end], scripts)
		require.NoError(t, err)
	}

	_, err := s.pool.Exec(ctx, `ANALYZE utxo`)
	require.NoError(t, err)

	// Seeding writes tens of megabytes. Without a checkpoint here, the one it provokes lands
	// inside whichever write follows, and the medians of the big blocks swing by a factor of two.
	// A superuser is needed; without one the swing stays and the run says so.
	if _, err := s.pool.Exec(ctx, `CHECKPOINT`); err != nil {
		t.Logf("no CHECKPOINT before the timed write, expect noise: %v", err)
	}
}

// precheck drops the transactions the store already holds, the way the caller's pre-check does.
func (b *benchBlock) precheck(t *testing.T, s *Store, ctx context.Context) []*bt.Tx {
	t.Helper()

	txids := make([][]byte, len(b.txs))
	for i, tx := range b.txs {
		h := tx.TxIDChainHash()
		txids[i] = h[:]
	}

	rows, err := s.pool.Query(ctx, `SELECT txid FROM tx_ident WHERE txid = ANY($1::bytea[])`, txids)
	require.NoError(t, err)

	held := map[chainhash.Hash]struct{}{}

	for rows.Next() {
		var id []byte
		require.NoError(t, rows.Scan(&id))

		held[chainhash.Hash(id)] = struct{}{}
	}

	rows.Close()
	require.NoError(t, rows.Err())

	out := make([]*bt.Tx, 0, len(b.txs))

	for _, tx := range b.txs {
		if _, ok := held[*tx.TxIDChainHash()]; !ok {
			out = append(out, tx)
		}
	}

	return out
}

// stmtTracer counts the statements a store's pool runs while armed, by kind, with their total
// time, which is summed across connections and so can exceed wall time.
type stmtTracer struct {
	mu    sync.Mutex
	armed bool
	snap  stmtSnapshot
}

type stmtSnapshot struct {
	statements int
	count      map[string]int
	dur        map[string]time.Duration
	rows       map[string]int64
	// first and last are the start of the earliest and the end of the latest statement of each
	// kind, so last-first is the span the kind occupied. dur divided by that span is how many
	// statements of the kind ran at once, on average.
	first map[string]time.Time
	last  map[string]time.Time
}

type stmtKey struct{}

type stmtStart struct {
	at   time.Time
	kind string
}

func (s *stmtTracer) begin() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.armed = true
	s.snap = stmtSnapshot{count: map[string]int{}, dur: map[string]time.Duration{}, rows: map[string]int64{},
		first: map[string]time.Time{}, last: map[string]time.Time{}}
}

func (s *stmtTracer) end() stmtSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.armed = false

	return s.snap
}

func kindOf(sql string) string {
	switch {
	case strings.EqualFold(sql, "begin"):
		return "begin"
	case strings.EqualFold(sql, "commit"):
		return "commit"
	case strings.EqualFold(sql, "rollback"):
		return "rollback"
	case strings.Contains(sql, "pg_advisory_xact_lock"):
		return "lock"
	case strings.Contains(sql, "AS t(leaf, txid, lo, hi)"):
		return "probe"
	case strings.Contains(sql, "DELETE FROM utxo u USING k"):
		return "spend"
	case strings.Contains(sql, "INSERT INTO tx_ident"):
		return "create"
	case strings.Contains(sql, "INSERT INTO spend_journal") && !strings.Contains(sql, "DELETE"):
		return "journal"
	default:
		return "other"
	}
}

func (s *stmtTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, stmtKey{}, stmtStart{at: time.Now(), kind: kindOf(data.SQL)})
}

func (s *stmtTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	st, ok := ctx.Value(stmtKey{}).(stmtStart)
	if !ok {
		return
	}

	d := time.Since(st.at)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.armed {
		return
	}

	s.snap.statements++
	s.snap.count[st.kind]++
	s.snap.dur[st.kind] += d
	s.snap.rows[st.kind] += data.CommandTag.RowsAffected()

	if f, ok := s.snap.first[st.kind]; !ok || st.at.Before(f) {
		s.snap.first[st.kind] = st.at
	}

	if end := st.at.Add(d); end.After(s.snap.last[st.kind]) {
		s.snap.last[st.kind] = end
	}
}
