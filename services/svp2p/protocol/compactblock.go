package protocol

import (
	"bytes"
	"context"
	"io"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// readStatus is ReadStatus_t (blockencodings.h:143-153).
// READ_STATUS_CHECKBLOCK_FAILED (blockencodings.h:152) is not carried.
//
//	readOK      READ_STATUS_OK       blockencodings.h:145
//	readInvalid READ_STATUS_INVALID  blockencodings.h:148
//	readFailed  READ_STATUS_FAILED   blockencodings.h:150
type readStatus int

const (
	readOK readStatus = iota
	readInvalid
	readFailed
)

// minCompactTxSize is MIN_TRANSACTION_SIZE (validation.h:67-68).
const minCompactTxSize = 10

const maxConsecutiveEmptyReads = 100

var (
	// ErrCompactBlockInvalid is READ_STATUS_INVALID (blockencodings.h:148).
	ErrCompactBlockInvalid = errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: invalid compact block")

	// ErrCompactFillFailed is READ_STATUS_FAILED (blockencodings.h:150).
	ErrCompactFillFailed = errors.New(errors.ERR_PROCESSING, "svp2p: compact block reconstruction failed")
)

// maxCompactBlockTxs is InitData's transaction-count ceiling
// (blockencodings.cpp:92-95).
func maxCompactBlockTxs() uint64 {
	return wire.MaxBlockPayload() / minCompactTxSize
}

// txSlot is one entry of PartiallyDownloadedBlock::txns_available
// (blockencodings.h:119-121).
type txSlot struct {
	prefilled []byte
	held      *chainhash.Hash
	shortID   uint64
}

func (s *txSlot) filled() bool { return s.prefilled != nil || s.held != nil }

// compactState is PartiallyDownloadedBlock (blockencodings.h:117-137).
type compactState struct {
	hash      chainhash.Hash
	header    wire.BlockHeader
	k0, k1    uint64
	txs       []txSlot
	missing   []uint32
	requested bool
	status    readStatus
	gaps      io.Reader
}

// newCompactState is PartiallyDownloadedBlock::InitData
// (blockencodings.cpp:84-242).
func newCompactState(m *wire.MsgCmpctBlock, idx TxIndex) (*compactState, readStatus, error) {
	// blockencodings.cpp:88-91 cmpctblock.header.IsNull(); CBlockHeader::IsNull
	// is nBits == 0 (primitives/block.h:60).
	if m.Header.Bits == 0 {
		return nil, readInvalid, nil
	}

	// blockencodings.cpp:88-91: no short IDs and no prefilled transactions.
	if len(m.ShortIDs) == 0 && len(m.PrefilledTxn) == 0 {
		return nil, readInvalid, nil
	}

	// blockencodings.cpp:92-95, before txns_available is resized at :105.
	total := uint64(len(m.ShortIDs)) + uint64(len(m.PrefilledTxn))
	if total > maxCompactBlockTxs() {
		return nil, readInvalid, nil
	}

	c := &compactState{
		hash:   m.Header.BlockHash(),
		header: m.Header,
		txs:    make([]txSlot, total),
	}

	c.k0, c.k1 = ShortIDKeys(&m.Header, m.Nonce)

	if status := c.placePrefilled(m); status != readOK {
		return nil, status, nil
	}

	slotOf, status := c.placeShortIDs(m)
	if status != readOK {
		return nil, status, nil
	}

	if err := c.matchIndex(m, idx, slotOf); err != nil {
		return nil, readFailed, err
	}

	c.collectMissing()

	return c, readOK, nil
}

// placePrefilled is InitData's prefilled loop (blockencodings.cpp:107-127).
func (c *compactState) placePrefilled(m *wire.MsgCmpctBlock) readStatus {
	last := int64(-1)

	for i, p := range m.PrefilledTxn {
		// blockencodings.cpp:114-117 accumulates lastprefilledindex from the
		// differential form, so it can never decrease.
		if int64(p.Index) <= last {
			return readInvalid
		}

		last = int64(p.Index)

		// blockencodings.cpp:110-112 prefilledtxn.tx->IsNull();
		// CTransaction::IsNull is vin.empty() && vout.empty()
		// (primitives/transaction.h:307).
		if p.Tx == nil || (len(p.Tx.TxIn) == 0 && len(p.Tx.TxOut) == 0) {
			return readInvalid
		}

		// blockencodings.cpp:119-125.
		if uint64(p.Index) > uint64(len(m.ShortIDs))+uint64(i) {
			return readInvalid
		}

		var buf bytes.Buffer

		if err := p.Tx.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
			return readInvalid
		}

		c.txs[p.Index].prefilled = buf.Bytes()
	}

	return readOK
}

// placeShortIDs is InitData's short ID map (blockencodings.cpp:140-169); the
// index_offset walk is blockencodings.cpp:142-146.
//
// Deviation: a duplicate short ID inside the message is READ_STATUS_FAILED at
// blockencodings.cpp:159-162 and READ_STATUS_INVALID here. The bucket_size > 12
// guard at blockencodings.cpp:150-153 is not ported.
func (c *compactState) placeShortIDs(m *wire.MsgCmpctBlock) ([]uint32, readStatus) {
	seen := make(map[uint64]struct{}, len(m.ShortIDs))
	slotOf := make([]uint32, len(m.ShortIDs))

	offset := 0

	for i, id := range m.ShortIDs {
		for c.txs[i+offset].filled() {
			offset++
		}

		if _, dup := seen[id]; dup {
			return nil, readInvalid
		}

		seen[id] = struct{}{}

		slot := i + offset
		slotOf[i] = uint32(slot) //nolint:gosec // slot < total, bounded above by maxCompactBlockTxs
		c.txs[slot].shortID = id
	}

	return slotOf, readOK
}

// matchIndex is InitData's mempool walk (blockencodings.cpp:171-199). A short
// ID two indexed hashes share leaves the slot empty and joins the gaps
// (blockencodings.cpp:174-183).
func (c *compactState) matchIndex(m *wire.MsgCmpctBlock, idx TxIndex, slotOf []uint32) error {
	if idx == nil {
		return nil
	}

	hashes, _ := idx.Match(c.k0, c.k1, m.ShortIDs)
	if len(hashes) != len(slotOf) {
		return errors.New(errors.ERR_PROCESSING,
			"svp2p: block %s index answered %d short ids of %d", c.hash, len(hashes), len(slotOf),
			ErrCompactFillFailed)
	}

	for i, h := range hashes {
		if h == nil {
			continue
		}

		c.txs[slotOf[i]].held = h
	}

	return nil
}

// collectMissing lists the getblocktxn indexes (net_processing.cpp:3864-3869).
func (c *compactState) collectMissing() {
	for i := range c.txs {
		if !c.txs[i].filled() {
			c.missing = append(c.missing, uint32(i)) //nolint:gosec // i < total, bounded above by maxCompactBlockTxs
		}
	}
}

// gapRequest is the getblocktxn for the missing slots
// (net_processing.cpp:3870-3881). The indexes are strictly increasing, which
// the differential encoding requires (blockencodings.h:36-82).
func (c *compactState) gapRequest() *wire.MsgGetBlockTxn {
	if len(c.missing) == 0 {
		return nil
	}

	c.requested = true

	indexes := make([]uint32, len(c.missing))
	copy(indexes, c.missing)

	return &wire.MsgGetBlockTxn{BlockHash: c.hash, Indexes: indexes}
}

// fill is FillBlock's arity check (blockencodings.cpp:264-285): too few fails
// at blockencodings.cpp:268-270, too many at blockencodings.cpp:283-285, both
// READ_STATUS_INVALID.
func (c *compactState) fill(count uint64, txs io.Reader) (readStatus, error) {
	if count != uint64(len(c.missing)) {
		c.status = readInvalid

		return readInvalid, errors.New(errors.ERR_NETWORK_PEER_MALICIOUS,
			"svp2p: blocktxn for %s carries %d transactions, %d requested", c.hash, count, len(c.missing),
			ErrCompactBlockInvalid)
	}

	c.gaps = txs

	return readOK, nil
}

func (c *compactState) fillStatus() readStatus { return c.status }

func (c *compactState) assemble(ctx context.Context, idx TxIndex) (uint64, io.ReadCloser) {
	var head bytes.Buffer

	_ = c.header.Serialize(&head)
	_ = wire.WriteVarInt(&head, wire.ProtocolVersion, uint64(len(c.txs)))

	a := c.newAssembler(ctx, idx)
	a.head = &head

	return uint64(len(c.txs)), a
}

func (c *compactState) assembleTxs(ctx context.Context, idx TxIndex) (uint64, io.ReadCloser) {
	return uint64(len(c.txs)), c.newAssembler(ctx, idx)
}

func (c *compactState) newAssembler(ctx context.Context, idx TxIndex) *compactAssembler {
	return &compactAssembler{ctx: ctx, state: c, idx: idx}
}

// compactAssembler is FillBlock's loop (blockencodings.cpp:271-281).
type compactAssembler struct {
	ctx   context.Context
	state *compactState
	idx   TxIndex

	head io.Reader
	cur  io.Reader
	raw  io.Reader
	open io.Closer
	next int
	err  error

	sized bool
	want  uint64
	got   uint64
}

func (a *compactAssembler) Read(p []byte) (int, error) {
	if a.err != nil {
		return 0, a.err
	}

	if len(p) == 0 {
		return 0, nil
	}

	if a.head != nil {
		n, err := a.head.Read(p)
		if errors.Is(err, io.EOF) {
			a.head = nil
			err = nil
		}

		if err != nil {
			a.err = err

			return n, err
		}

		if n > 0 {
			return n, nil
		}
	}

	idle := 0

	for {
		if a.cur == nil {
			if err := a.advance(); err != nil {
				a.err = err

				return 0, err
			}
		}

		n, err := a.cur.Read(p)
		a.got += uint64(n) //nolint:gosec // n is never negative

		if errors.Is(err, io.EOF) {
			err = a.endSlot()
			idle = 0
		}

		if err != nil {
			a.err = err

			return n, err
		}

		if n > 0 {
			return n, nil
		}

		idle++
		if idle >= maxConsecutiveEmptyReads {
			a.err = a.fail("svp2p: block %s slot %d returned no bytes in %d reads: %v",
				a.state.hash, a.next-1, idle, io.ErrNoProgress)

			return 0, a.err
		}
	}
}

func (a *compactAssembler) Close() error {
	a.release()

	if a.err == nil {
		a.err = io.EOF
	}

	return nil
}

func (a *compactAssembler) release() {
	if a.open != nil {
		_ = a.open.Close()
		a.open = nil
	}

	a.cur = nil
	a.raw = nil
	a.sized = false
	a.want = 0
	a.got = 0
}

func (a *compactAssembler) endSlot() error {
	sized, want, got, slot := a.sized, a.want, a.got, a.next-1
	over := sized && got == want && a.overruns()

	a.release()

	if sized && got != want {
		return a.fail("svp2p: block %s slot %d received %d bytes of the %d the index declared",
			a.state.hash, slot, got, want)
	}

	if over {
		return a.fail("svp2p: block %s slot %d holds more than the %d bytes the index declared",
			a.state.hash, slot, want)
	}

	return nil
}

func (a *compactAssembler) overruns() bool {
	if a.raw == nil {
		return false
	}

	var probe [1]byte

	n, _ := a.raw.Read(probe[:])

	return n > 0
}

// advance opens the next slot's source. blockencodings.cpp:271-281 walks
// txns_available in the same order, taking a transaction from vtx_missing
// whenever the slot is empty.
func (a *compactAssembler) advance() error {
	if a.next >= len(a.state.txs) {
		return io.EOF
	}

	slot := &a.state.txs[a.next]
	a.next++

	switch {
	case slot.prefilled != nil:
		a.cur = bytes.NewReader(slot.prefilled)

		return nil

	case slot.held != nil:
		return a.openHeld(*slot.held)

	default:
		return a.readGap(slot.shortID)
	}
}

func (a *compactAssembler) openHeld(hash chainhash.Hash) error {
	if a.idx == nil {
		return a.fail("svp2p: block %s needs held transaction %s with no index", a.state.hash, hash)
	}

	rc, size, err := a.idx.Open(a.ctx, hash)
	if err != nil {
		return a.fail("svp2p: block %s cannot open held transaction %s: %v", a.state.hash, hash, err)
	}

	a.open = rc
	a.raw = rc
	a.cur = io.LimitReader(rc, int64(size)) //nolint:gosec // size is the store's own byte count
	a.sized = true
	a.want = size
	a.got = 0

	return nil
}

// readGap is the vtx_missing side of FillBlock (blockencodings.cpp:271-281).
// SVNode reaches READ_STATUS_FAILED for a wrong transaction only through
// CheckBlock's merkle root and CorruptionPossible (blockencodings.cpp:288-298);
// this port checks the slot's short ID directly.
func (a *compactAssembler) readGap(shortID uint64) error {
	if a.state.gaps == nil {
		return a.fail("svp2p: block %s needs a gap transaction with no blocktxn stream", a.state.hash)
	}

	var buf bytes.Buffer

	tx := &wire.MsgTx{}
	if err := tx.Bsvdecode(io.TeeReader(a.state.gaps, &buf), wire.ProtocolVersion, wire.BaseEncoding); err != nil {
		return a.invalid("svp2p: block %s cannot decode a gap transaction: %v", a.state.hash, err)
	}

	raw := buf.Bytes()

	if got := ShortID(a.state.k0, a.state.k1, chainhash.DoubleHashH(raw)); got != shortID {
		return a.fail("svp2p: block %s gap transaction has short id %d, slot %d wants %d",
			a.state.hash, got, a.next-1, shortID)
	}

	a.cur = bytes.NewReader(raw)

	return nil
}

func (a *compactAssembler) fail(format string, args ...any) error {
	a.state.status = readFailed

	return errors.New(errors.ERR_PROCESSING, format, append(args, ErrCompactFillFailed)...)
}

// invalid is the streaming half of FillBlock's arity check
// (blockencodings.cpp:268-270, blockencodings.cpp:283-285).
func (a *compactAssembler) invalid(format string, args ...any) error {
	a.state.status = readInvalid

	return errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, format, append(args, ErrCompactBlockInvalid)...)
}
