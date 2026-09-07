package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// compactFixtureTxCount sizes the fixture block so the index_offset walk
// (blockencodings.cpp:140-146) runs over many slots.
const compactFixtureTxCount = 50

const compactFixtureNonce = uint64(0x0123456789abcdef)

func compactFixtureTx(i int) *wire.MsgTx {
	tx := wire.NewMsgTx(1)

	var prev chainhash.Hash

	binary.LittleEndian.PutUint64(prev[:8], uint64(i)+1) //nolint:gosec // fixture indexes are small

	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&prev, uint32(i)), []byte{0x51, byte(i)})) //nolint:gosec // fixture indexes are small
	tx.AddTxOut(wire.NewTxOut(int64(1000+i), []byte{0x76, 0xa9, byte(i), 0x88, 0xac}))

	return tx
}

// compactFixtureBlock builds a block of n transactions with a non-null header
// (Bits != 0; CBlockHeader::IsNull, primitives/block.h:60).
func compactFixtureBlock(t *testing.T, n int) *wire.MsgBlock {
	t.Helper()

	var prev, merkle chainhash.Hash

	prev[0] = 0xaa
	merkle[0] = 0xbb

	blk := wire.NewMsgBlock(wire.NewBlockHeader(1, &prev, &merkle, 0x207fffff, 7))

	for i := 0; i < n; i++ {
		require.NoError(t, blk.AddTransaction(compactFixtureTx(i)))
	}

	return blk
}

func encodeTx(t *testing.T, tx *wire.MsgTx) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, tx.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	return buf.Bytes()
}

func encodeBlock(t *testing.T, blk *wire.MsgBlock) []byte {
	t.Helper()

	var buf bytes.Buffer

	require.NoError(t, blk.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	return buf.Bytes()
}

func compactMsgFor(t *testing.T, blk *wire.MsgBlock, prefilled ...int) *wire.MsgCmpctBlock {
	t.Helper()

	isPrefilled := make(map[int]bool, len(prefilled))
	for _, i := range prefilled {
		isPrefilled[i] = true
	}

	msg := &wire.MsgCmpctBlock{Header: blk.Header, Nonce: compactFixtureNonce}
	k0, k1 := ShortIDKeys(&blk.Header, msg.Nonce)

	for i, tx := range blk.Transactions {
		if isPrefilled[i] {
			require.NoError(t, msg.AddPrefilledTransaction(uint32(i), tx)) //nolint:gosec // fixture indexes are small

			continue
		}

		msg.ShortIDs = append(msg.ShortIDs, ShortID(k0, k1, tx.TxHash()))
	}

	return msg
}

type compactTestIndex struct {
	raw       map[chainhash.Hash][]byte
	openFails map[chainhash.Hash]bool
	shortBy   map[chainhash.Hash]int
	extraBy   map[chainhash.Hash]int
	stallAt   map[chainhash.Hash]bool
	// collideAt marks a short ID two indexed hashes share
	// (blockencodings.cpp:174-183).
	collideAt     map[int]bool
	truncateMatch bool
}

func newCompactTestIndex() *compactTestIndex {
	return &compactTestIndex{
		raw:       make(map[chainhash.Hash][]byte),
		openFails: make(map[chainhash.Hash]bool),
		shortBy:   make(map[chainhash.Hash]int),
		extraBy:   make(map[chainhash.Hash]int),
		stallAt:   make(map[chainhash.Hash]bool),
		collideAt: make(map[int]bool),
	}
}

func (f *compactTestIndex) hold(t *testing.T, tx *wire.MsgTx) {
	t.Helper()

	f.raw[tx.TxHash()] = encodeTx(t, tx)
}

func (f *compactTestIndex) Match(k0, k1 uint64, shortIDs []uint64) ([]*chainhash.Hash, bool) {
	byShort := make(map[uint64]chainhash.Hash, len(f.raw))
	for h := range f.raw {
		byShort[ShortID(k0, k1, h)] = h
	}

	out := make([]*chainhash.Hash, len(shortIDs))
	collision := false

	for i, id := range shortIDs {
		if f.collideAt[i] {
			collision = true

			continue
		}

		if h, ok := byShort[id]; ok {
			found := h
			out[i] = &found
		}
	}

	if f.truncateMatch && len(out) > 0 {
		out = out[:len(out)-1]
	}

	return out, collision
}

func (f *compactTestIndex) Open(_ context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error) {
	if f.openFails[hash] {
		return nil, 0, ErrTxUnknown
	}

	raw, ok := f.raw[hash]
	if !ok {
		return nil, 0, ErrTxUnknown
	}

	if f.stallAt[hash] {
		return io.NopCloser(stalledReader{}), uint64(len(raw)), nil
	}

	if n := f.shortBy[hash]; n > 0 {
		return io.NopCloser(bytes.NewReader(raw[:len(raw)-n])), uint64(len(raw)), nil
	}

	if n := f.extraBy[hash]; n > 0 {
		over := append(append([]byte(nil), raw...), bytes.Repeat([]byte{0xee}, n)...)

		return io.NopCloser(bytes.NewReader(over)), uint64(len(raw)), nil
	}

	return io.NopCloser(bytes.NewReader(raw)), uint64(len(raw)), nil
}

type stalledReader struct{}

func (stalledReader) Read([]byte) (int, error) { return 0, nil }

func gapStream(t *testing.T, blk *wire.MsgBlock, indexes []uint32) io.Reader {
	t.Helper()

	var buf bytes.Buffer

	for _, i := range indexes {
		buf.Write(encodeTx(t, blk.Transactions[i]))
	}

	return &buf
}

func drain(t *testing.T, rc io.ReadCloser) ([]byte, error) {
	t.Helper()

	out, err := io.ReadAll(rc)
	require.NoError(t, rc.Close())

	return out, err
}

// TestCompactState_AllHeld_AssemblesWithoutGapRequest is the no-getblocktxn
// case (net_processing.cpp:3870-3881).
func TestCompactState_AllHeld_AssemblesWithoutGapRequest(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)
	require.NotNil(t, c)
	require.Empty(t, c.missing)
	require.Nil(t, c.gapRequest())

	count, rc := c.assemble(context.Background(), idx)
	require.Equal(t, uint64(compactFixtureTxCount), count)

	got, err := drain(t, rc)
	require.NoError(t, err)
	require.Equal(t, encodeBlock(t, blk), got)
}

// TestCompactState_HalfHeld_GapRequestIndexes pins the getblocktxn indexes as
// strictly increasing, which the differential encoding requires
// (blockencodings.h:36-82).
func TestCompactState_HalfHeld_GapRequestIndexes(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()

	var want []uint32

	for i := 1; i < len(blk.Transactions); i++ {
		if i%2 == 0 {
			idx.hold(t, blk.Transactions[i])

			continue
		}

		want = append(want, uint32(i)) //nolint:gosec // fixture indexes are small
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)
	require.Equal(t, blk.BlockHash(), req.BlockHash)
	require.Equal(t, want, req.Indexes)
	require.True(t, c.requested)

	var buf bytes.Buffer

	require.NoError(t, req.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	var round wire.MsgGetBlockTxn

	require.NoError(t, round.Bsvdecode(&buf, wire.ProtocolVersion, wire.BaseEncoding))
	require.Equal(t, want, round.Indexes)
}

func TestCompactState_FillAndAssemble(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0, 17, 33)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		if i != 17 && i != 33 && i%3 != 0 {
			idx.hold(t, blk.Transactions[i])
		}
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)
	require.NotEmpty(t, req.Indexes)

	gaps := gapStream(t, blk, req.Indexes)

	fillStatus, err := c.fill(uint64(len(req.Indexes)), gaps)
	require.NoError(t, err)
	require.Equal(t, readOK, fillStatus)

	count, rc := c.assemble(context.Background(), idx)
	require.Equal(t, uint64(compactFixtureTxCount), count)

	got, drainErr := drain(t, rc)
	require.NoError(t, drainErr)
	require.Equal(t, encodeBlock(t, blk), got)
	require.Equal(t, readOK, c.fillStatus())
}

func TestCompactState_AssembleTxs_OmitsHeaderAndCount(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	count, rc := c.assembleTxs(context.Background(), idx)
	require.Equal(t, uint64(compactFixtureTxCount), count)

	got, err := drain(t, rc)
	require.NoError(t, err)

	body := encodeBlock(t, blk)
	require.Equal(t, body[len(body)-len(got):], got)
	require.Less(t, len(got), len(body))
}

// TestCompactState_Fill_WrongCount is FillBlock's count check
// (blockencodings.cpp:268-270, blockencodings.cpp:283-285).
func TestCompactState_Fill_WrongCount(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i += 2 {
		idx.hold(t, blk.Transactions[i])
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)

	for _, count := range []uint64{uint64(len(req.Indexes)) - 1, uint64(len(req.Indexes)) + 1, 0} {
		fillStatus, err := c.fill(count, bytes.NewReader(nil))
		require.Equal(t, readInvalid, fillStatus)
		require.Error(t, err)
	}
}

// TestCompactState_Fill_ShortIDMismatch is the "Possible Short ID collision"
// READ_STATUS_FAILED of blockencodings.cpp:288-298.
func TestCompactState_Fill_ShortIDMismatch(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		if i%2 == 0 {
			idx.hold(t, blk.Transactions[i])
		}
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)
	require.NotEmpty(t, req.Indexes)

	var buf bytes.Buffer

	buf.Write(encodeTx(t, compactFixtureTx(9999)))

	for _, i := range req.Indexes[1:] {
		buf.Write(encodeTx(t, blk.Transactions[i]))
	}

	fillStatus, err := c.fill(uint64(len(req.Indexes)), &buf)
	require.NoError(t, err)
	require.Equal(t, readOK, fillStatus)

	_, rc := c.assemble(context.Background(), idx)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readFailed, c.fillStatus())
}

// TestCompactState_DuplicateShortIDs is InitData's short ID map check
// (blockencodings.cpp:148-169), READ_STATUS_FAILED there and readInvalid here.
func TestCompactState_DuplicateShortIDs(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	msg.ShortIDs[7] = msg.ShortIDs[3]

	c, status, err := newCompactState(msg, newCompactTestIndex())
	require.NoError(t, err)
	require.Equal(t, readInvalid, status)
	require.Nil(t, c)
}

// TestCompactState_PrefilledIndexOutOfRange is blockencodings.cpp:119-125.
func TestCompactState_PrefilledIndexOutOfRange(t *testing.T) {
	blk := compactFixtureBlock(t, 4)
	msg := compactMsgFor(t, blk, 0)

	msg.PrefilledTxn[0].Index = uint32(len(msg.ShortIDs) + 1)

	c, status, err := newCompactState(msg, newCompactTestIndex())
	require.NoError(t, err)
	require.Equal(t, readInvalid, status)
	require.Nil(t, c)
}

// TestCompactState_IndexCollision_IsMissingNotFailed is
// blockencodings.cpp:174-183: the slot is cleared and requested, not failed.
func TestCompactState_IndexCollision_IsMissingNotFailed(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.collideAt[11] = true

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)
	require.Equal(t, []uint32{12}, req.Indexes)

	gaps := gapStream(t, blk, req.Indexes)

	fillStatus, fillErr := c.fill(1, gaps)
	require.NoError(t, fillErr)
	require.Equal(t, readOK, fillStatus)

	_, rc := c.assemble(context.Background(), idx)

	got, drainErr := drain(t, rc)
	require.NoError(t, drainErr)
	require.Equal(t, encodeBlock(t, blk), got)
}

func TestCompactState_HeldTxGoneAtAssembly(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.openFails[blk.Transactions[20].TxHash()] = true

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)
	require.Nil(t, c.gapRequest())

	_, rc := c.assemble(context.Background(), idx)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readFailed, c.fillStatus())
}

func TestCompactState_InvalidMessages(t *testing.T) {
	blk := compactFixtureBlock(t, 4)

	nullHeader := compactMsgFor(t, blk, 0)
	nullHeader.Header.Bits = 0

	empty := &wire.MsgCmpctBlock{Header: blk.Header, Nonce: compactFixtureNonce}

	nullPrefilled := compactMsgFor(t, blk, 0)
	nullPrefilled.PrefilledTxn[0].Tx = wire.NewMsgTx(1)

	tests := []struct {
		name string
		msg  *wire.MsgCmpctBlock
	}{
		// blockencodings.cpp:88-91, primitives/block.h:60 (nBits == 0).
		{name: "null header", msg: nullHeader},
		// blockencodings.cpp:88-91: no short IDs and no prefilled txn.
		{name: "no transactions", msg: empty},
		// blockencodings.cpp:110-112, primitives/transaction.h:307.
		{name: "null prefilled tx", msg: nullPrefilled},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, status, err := newCompactState(tc.msg, newCompactTestIndex())
			require.NoError(t, err)
			require.Equal(t, readInvalid, status)
			require.Nil(t, c)
		})
	}
}

// TestCompactState_TooManyTransactions is InitData's count bound
// (blockencodings.cpp:92-95; MIN_TRANSACTION_SIZE, validation.h:67-68).
func TestCompactState_TooManyTransactions(t *testing.T) {
	restore := wire.MaxBlockPayload()

	wire.SetLimits(1000000)

	t.Cleanup(func() { wire.SetLimits(restore) })

	blk := compactFixtureBlock(t, 2)

	msg := compactMsgFor(t, blk, 0)
	msg.ShortIDs = make([]uint64, maxCompactBlockTxs()+1)

	for i := range msg.ShortIDs {
		msg.ShortIDs[i] = uint64(i) + 1 //nolint:gosec // fixture indexes are small
	}

	c, status, err := newCompactState(msg, newCompactTestIndex())
	require.NoError(t, err)
	require.Equal(t, readInvalid, status)
	require.Nil(t, c)
}

func TestCompactState_NoIndex_RequestsEverything(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	c, status, err := newCompactState(msg, nil)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)
	require.Len(t, req.Indexes, compactFixtureTxCount-1)

	gaps := gapStream(t, blk, req.Indexes)

	fillStatus, err := c.fill(uint64(len(req.Indexes)), gaps)
	require.NoError(t, err)
	require.Equal(t, readOK, fillStatus)

	_, rc := c.assemble(context.Background(), nil)

	got, drainErr := drain(t, rc)
	require.NoError(t, drainErr)
	require.Equal(t, encodeBlock(t, blk), got)
}

func TestCompactState_GapStreamTruncated(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	c, status, err := newCompactState(msg, nil)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)

	full, err := io.ReadAll(gapStream(t, blk, req.Indexes))
	require.NoError(t, err)

	gaps := bytes.NewReader(full[:len(full)/2])

	fillStatus, fillErr := c.fill(uint64(len(req.Indexes)), gaps)
	require.NoError(t, fillErr)
	require.Equal(t, readOK, fillStatus)

	_, rc := c.assemble(context.Background(), nil)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactBlockInvalid)
	require.NotErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readInvalid, c.fillStatus())
}

// TestCompactState_PrefilledIndexesNotIncreasing is the monotonicity
// blockencodings.cpp:114-117 accumulates from the differential encoding.
func TestCompactState_PrefilledIndexesNotIncreasing(t *testing.T) {
	blk := compactFixtureBlock(t, 6)
	msg := compactMsgFor(t, blk, 0, 2)

	msg.PrefilledTxn[1].Index = msg.PrefilledTxn[0].Index

	c, status, err := newCompactState(msg, newCompactTestIndex())
	require.NoError(t, err)
	require.Equal(t, readInvalid, status)
	require.Nil(t, c)
}

func TestCompactState_HeldTxUnderSupplied(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.shortBy[blk.Transactions[20].TxHash()] = 3

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)
	require.Nil(t, c.gapRequest())

	_, rc := c.assemble(context.Background(), idx)

	got, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readFailed, c.fillStatus())
	require.NotEqual(t, encodeBlock(t, blk), got)
}

func TestCompactState_GapRequest_CopiesAndRepeats(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 2; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	first := c.gapRequest()
	require.NotNil(t, first)

	want := append([]uint32(nil), first.Indexes...)
	require.NotEmpty(t, want)

	first.Indexes[0] = 0xffffffff
	first.Indexes = first.Indexes[:0]

	second := c.gapRequest()
	require.NotNil(t, second)
	require.Equal(t, want, second.Indexes)
	require.Equal(t, want, c.missing)
	require.True(t, c.requested)
}

func TestCompactState_IndexMatchWrongLength(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.truncateMatch = true

	c, status, err := newCompactState(msg, idx)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrCompactFillFailed)
	require.Equal(t, readFailed, status)
	require.Nil(t, c)
}

func TestCompactState_Fill_LatchesStream(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		if i%2 == 0 {
			idx.hold(t, blk.Transactions[i])
		}
	}

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	req := c.gapRequest()
	require.NotNil(t, req)

	fillStatus, fillErr := c.fill(uint64(len(req.Indexes)), gapStream(t, blk, req.Indexes))
	require.NoError(t, fillErr)
	require.Equal(t, readOK, fillStatus)

	_, rc := c.assemble(context.Background(), idx)

	got, drainErr := drain(t, rc)
	require.NoError(t, drainErr)
	require.Equal(t, encodeBlock(t, blk), got)
	require.Equal(t, readOK, c.fillStatus())
}

func TestCompactState_AssembleWithoutFill(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	c, status, err := newCompactState(msg, nil)
	require.NoError(t, err)
	require.Equal(t, readOK, status)
	require.NotNil(t, c.gapRequest())

	_, rc := c.assemble(context.Background(), nil)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readFailed, c.fillStatus())
}

func TestCompactState_HeldTxOverSupplied(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.extraBy[blk.Transactions[20].TxHash()] = 3

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)
	require.Nil(t, c.gapRequest())

	_, rc := c.assemble(context.Background(), idx)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Equal(t, readFailed, c.fillStatus())
}

func TestCompactState_HeldTxStalls(t *testing.T) {
	blk := compactFixtureBlock(t, compactFixtureTxCount)
	msg := compactMsgFor(t, blk, 0)

	idx := newCompactTestIndex()
	for i := 1; i < len(blk.Transactions); i++ {
		idx.hold(t, blk.Transactions[i])
	}

	idx.stallAt[blk.Transactions[20].TxHash()] = true

	c, status, err := newCompactState(msg, idx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	_, rc := c.assemble(context.Background(), idx)

	_, drainErr := drain(t, rc)
	require.Error(t, drainErr)
	require.ErrorIs(t, drainErr, ErrCompactFillFailed)
	require.Contains(t, drainErr.Error(), io.ErrNoProgress.Error())
	require.Equal(t, readFailed, c.fillStatus())
}

// TestCompactState_PrefilledIndexAtAcceptingEdge is the accepting edge of
// blockencodings.cpp:119-125: p.Index == len(ShortIDs)+i.
func TestCompactState_PrefilledIndexAtAcceptingEdge(t *testing.T) {
	tests := []struct {
		name      string
		txCount   int
		prefilled []int
		move      int
	}{
		{name: "one prefilled, i=0", txCount: 4, prefilled: []int{0}, move: 0},
		{name: "two prefilled, i=1", txCount: 6, prefilled: []int{0, 2}, move: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blk := compactFixtureBlock(t, tc.txCount)
			msg := compactMsgFor(t, blk, tc.prefilled...)

			edge := uint32(len(msg.ShortIDs) + tc.move) //nolint:gosec // fixture indexes are small
			msg.PrefilledTxn[tc.move].Index = edge

			c, status, err := newCompactState(msg, newCompactTestIndex())
			require.NoError(t, err)
			require.Equal(t, readOK, status)
			require.NotNil(t, c)
			require.Len(t, c.txs, tc.txCount)
			require.NotNil(t, c.txs[edge].prefilled)
			require.NotContains(t, c.missing, edge)
		})
	}
}
