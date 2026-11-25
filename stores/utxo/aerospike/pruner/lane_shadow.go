package pruner

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// The pruner's lane rule on Aerospike, at utxostore_laneMigration shadow (lanes Phase 1, task P4).
//
// Aerospike's pruner decides in Go: the delete-at-height query streams each candidate record to
// processRecordChunk, which keeps an unmined record, keeps a parent whose child is unstable in
// defensive mode, and deletes the rest in one batch per chunk. The lane rule is judged there, on the
// same records, after today's decisions and before the delete batch, so a paged parent's pages can
// still be read. Nothing acts on the lane rule's answer; it is counted beside today's decision.
//
// One verdict per transaction. A master record is judged with every page's slots: its own lanePos
// bin is the parent's position, and each spent output's spendLanePos entry, on the master or the page
// holding the output, is its spender's. The pages a master names are read in one batch per chunk,
// only for the lane rule (teranode_lane_pruner_extra_reads_total kind page). A page record the query
// returns on its own is judged with its master, not again. A page another worker has already
// deleted, which only happens when today deletes its master, makes the master cannot_judge
// spender_unread.
//
// At off no Reader exists, newLaneShadow returns nil, the query asks for exactly the bins it asked
// for before, and every method below does nothing.

// laneShadow judges one chunk's decisions.
type laneShadow struct {
	s       *Service
	judge   *lanereader.PruneJudge
	records []*aerospike.Result
	// deleted are the records today deletes, in chunk order: processRecordChunk keeps the chunk's
	// order through every step, so run walks both lists together instead of hashing each record.
	deleted []*aerospike.Result
}

// newLaneShadow returns the chunk's judge, nil at off.
func (s *Service) newLaneShadow(chunk []*aerospike.Result) *laneShadow {
	j := lanereader.Current().PruneJudge(lanereader.PruneStoreAerospike, s.laneRetention)
	if j == nil {
		return nil
	}

	return &laneShadow{s: s, judge: j, records: chunk, deleted: make([]*aerospike.Result, 0, len(chunk))}
}

// laneQueryBins are the bins the lane rule needs beside today's: the parent's position, the slot
// positions and, outside defensive mode, the utxos bin, which says which outputs are spent and by
// whom.
func (s *Service) laneQueryBins(binNames []string) []string {
	binNames = append(binNames, fields.LanePosition.String(), fields.SpendingLanePositions.String())

	if !s.defensiveEnabled {
		binNames = append(binNames, s.fieldUtxos)
	}

	return binNames
}

// markDeleted records that today deletes rec.
func (l *laneShadow) markDeleted(rec *aerospike.Result) {
	if l != nil {
		l.deleted = append(l.deleted, rec)
	}
}

// laneMaster is one master record waiting for its pages.
type laneMaster struct {
	c     lanereader.PruneCandidate
	today bool
	pages []*aerospike.Key
}

// The lane bins' names, resolved once.
var (
	binLanePos      = fields.LanePosition.String()
	binSpendLanePos = fields.SpendingLanePositions.String()
)

// run judges every master record of the chunk and adds the counts to the metrics. It must run before
// the chunk's delete batch, so the pages it reads are still there.
func (l *laneShadow) run() {
	if l == nil {
		return
	}

	defer l.judge.Done()

	start := time.Now()

	masters := make([]laneMaster, 0, len(l.records))
	// Every master's spenders share one array; a page's spenders, appended later, copy that master's
	// out of it, since each master's slice is capped at its own end.
	spenders := make([]lanereader.PruneSpender, 0, 2*len(l.records))

	var (
		pageKeys int
		next     int // the next record of deleted to meet
	)

	for _, rec := range l.records {
		today := next < len(l.deleted) && l.deleted[next] == rec
		if today {
			next++
		}

		if rec.Err != nil || rec.Record == nil || rec.Record.Bins == nil {
			l.judge.Unjudged("record_error", 1)

			continue
		}

		bins := rec.Record.Bins

		txID := bytesBin(bins[l.s.fieldTxID])
		if len(txID) != chainhash.HashSize {
			l.judge.Unjudged("record_error", 1)

			continue
		}

		extra, isMaster := bins[l.s.fieldTotalExtraRecs]
		if !isMaster {
			// No totalExtraRecs: a master from a writer that does not set it, stored under the bare
			// txid, or a page, judged with its master.
			key, kerr := aerospike.NewKey(l.s.namespace, l.s.set, uaerospike.CalculateKeySourceInternal((*chainhash.Hash)(txID), 0))
			if kerr != nil || !bytes.Equal(key.Digest(), rec.Record.Key.Digest()) {
				continue
			}
		}

		conflicting, _ := bins[l.s.fieldConflicting].(bool)

		masters = append(masters, laneMaster{today: today})
		m := &masters[len(masters)-1]
		copy(m.c.Parent[:], txID)
		m.c.ParentPos = binLanePosition(bins[binLanePos])
		m.c.Conflicting = conflicting

		start := len(spenders)
		spenders = appendSlotSpenders(spenders, bins[l.s.fieldUtxos], bins[binSpendLanePos])
		m.c.Spenders = spenders[start:len(spenders):len(spenders)]

		if n, _ := extra.(int); n > 0 {
			for num := 1; num <= n; num++ {
				key, kerr := aerospike.NewKey(l.s.namespace, l.s.set, uaerospike.CalculateKeySourceInternal(&m.c.Parent, uint32(num))) //nolint:gosec // a page number is small
				if kerr != nil {
					m.c.Unread = true

					continue
				}

				m.pages = append(m.pages, key)
				pageKeys++
			}
		}
	}

	if pageKeys > 0 {
		// The page reads are I/O, counted as reads, not as the rule's time.
		l.judge.Time(start)
		l.readPages(masters, pageKeys)
		start = time.Now()
	}

	for i := range masters {
		l.judge.Judge(masters[i].today, &masters[i].c)
	}

	l.judge.Time(start)
}

// readPages reads the utxos and slot positions of every master's pages in one batch and adds their
// spenders to the master. A page that cannot be read leaves its master unread.
func (l *laneShadow) readPages(masters []laneMaster, n int) {
	readPolicy := aerospike.NewBatchReadPolicy()
	readPolicy.ReadModeSC = aerospike.ReadModeSCSession

	bins := []string{l.s.fieldUtxos, binSpendLanePos}
	records := make([]aerospike.BatchRecordIfc, 0, n)

	for i := range masters {
		for _, key := range masters[i].pages {
			records = append(records, aerospike.NewBatchRead(readPolicy, key, bins))
		}
	}

	l.judge.Reads("page", len(records))

	if err := l.s.batchOperate(records); err != nil {
		for i := range masters {
			if len(masters[i].pages) > 0 {
				masters[i].c.Unread = true
			}
		}

		return
	}

	i := 0

	for j := range masters {
		m := &masters[j]

		for range m.pages {
			br := records[i].BatchRec()
			i++

			if br.Err != nil || br.Record == nil || br.Record.Bins == nil {
				m.c.Unread = true

				continue
			}

			m.c.Spenders = appendSlotSpenders(m.c.Spenders, br.Record.Bins[l.s.fieldUtxos], br.Record.Bins[binSpendLanePos])
		}
	}
}

// binLanePosition decodes a record's lanePos bin, lane.None when absent or not an integer.
func binLanePosition(v interface{}) uint64 {
	var p uint64

	switch t := v.(type) {
	case int:
		p = lane.FromSigned(int64(t))
	case int64:
		p = lane.FromSigned(t)
	default:
		return lane.None
	}

	if lane.CheckPosition(p) != nil {
		return lane.None
	}

	return p
}

// utxoSpendingDataEnd is where a spent utxo entry's spending data ends: the 32-byte utxo hash, the
// spender's 32-byte hash and its 4-byte input index.
const utxoSpendingDataEnd = 68

// appendSlotSpenders appends the spender of every spent output of one record: utxos is the record's
// utxos bin and positions its spendLanePos bin, eight bytes per output, zero for none. A frozen
// output has no spender. A positions bin that is malformed gives every spender no position.
func appendSlotSpenders(dst []lanereader.PruneSpender, utxos, positions interface{}) []lanereader.PruneSpender {
	list, _ := utxos.([]interface{})
	pos, _ := positions.([]byte)

	valid := len(pos)%lane.PositionSize == 0 && len(pos)/lane.PositionSize <= len(list)

	for i, raw := range list {
		b, ok := raw.([]byte)
		if !ok || len(b) < utxoSpendingDataEnd {
			continue
		}

		spender := b[32:64]
		if allZero(spender) || bytes.Equal(spender, subtreepkg.FrozenBytesTxHash[:]) {
			continue
		}

		s := lanereader.PruneSpender{Pos: lane.None}
		copy(s.Hash[:], spender)

		if valid && (i+1)*lane.PositionSize <= len(pos) {
			if p := binary.LittleEndian.Uint64(pos[i*lane.PositionSize:]); lane.CheckPosition(p) == nil {
				s.Pos = p
			}
		}

		dst = append(dst, s)
	}

	return dst
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}

	return true
}
