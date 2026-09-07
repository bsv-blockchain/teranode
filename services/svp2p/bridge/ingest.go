package bridge

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge/bsvutil"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// svp2pBridge implements the interface the protocol layer is shaped against.
// The assertion was deferred by the task that declared IngestBlock without a
// body; IngestBlock below is that body.
var _ Bridge = (*svp2pBridge)(nil)

// streamDecodeBufferBytes is the read-ahead the streaming decoder keeps over
// the transport's payload reader. Per-field socket reads would otherwise cost
// a syscall per varint. The buffer is bounded by the transport's declared
// payload length, so it can never read past the block into the next message.
const streamDecodeBufferBytes = 64 * 1024

// txSource yields a block's transactions in block order, one at a time. It is
// the seam that lets one pipeline serve both ingestion entries: the buffered
// entry (HandleBlockDirect) walks an already-decoded slice, and the streaming
// entry (IngestBlock) decodes each transaction off the wire only when the
// pipeline asks for it, so a multi-GB block is never materialized as a
// wire.MsgBlock. Next reports io.EOF once the block's declared transaction
// count has been yielded.
type txSource interface {
	Next() (*bsvutil.Tx, error)
}

// sourceBlock is the scalar identity the pipeline needs when transactions
// arrive from a txSource rather than a decoded *bsvutil.Block. height stays
// int32 — the type bsvutil.Block.Height() returns — so the uint32 conversion
// keeps happening at exactly the point prepareSubtrees always did it.
type sourceBlock struct {
	hash      chainhash.Hash
	prevBlock chainhash.Hash
	height    int32
	timestamp time.Time
	txCount   int
}

// sliceTxSource walks an already-decoded block's transactions. It is what the
// buffered entry hands to the shared pipeline.
type sliceTxSource struct {
	txs  []*bsvutil.Tx
	next int
}

func newSliceTxSource(txs []*bsvutil.Tx) *sliceTxSource {
	return &sliceTxSource{txs: txs}
}

func (s *sliceTxSource) Next() (*bsvutil.Tx, error) {
	if s.next >= len(s.txs) {
		return nil, io.EOF
	}

	tx := s.txs[s.next]
	s.next++

	return tx, nil
}

// streamTxSource decodes transactions one at a time from the transport's
// bounded payload reader. It retains exactly one decoded transaction — the
// coinbase, which IngestBlock has to serialize into the model block before the
// pipeline runs — and releases even that as soon as the pipeline takes it. The
// peak resident cost of a streaming ingest is therefore the txMap the pipeline
// builds, never the whole wire block. The buffered path already released its
// decode arena immediately after createTxMap (PR 1081), so a single streaming
// pass preserves that memory contract rather than weakening it.
type streamTxSource struct {
	r        *bufio.Reader
	txCount  uint64
	yielded  uint64
	coinbase *bsvutil.Tx

	// txBytes accumulates each transaction's serialized size as it is
	// decoded. It is how the streaming entry recovers the block's serialized
	// size, which the buffered entry read straight off the decoded block.
	txBytes int64
}

// newStreamTxSource decodes the coinbase up front, because IngestBlock needs
// it before the pipeline starts — the same order the buffered entry uses,
// where the coinbase is serialized out of the decoded block before
// prepareSubtrees runs.
func newStreamTxSource(r io.Reader, txCount uint64) (*streamTxSource, error) {
	s := &streamTxSource{
		r:       bufio.NewReaderSize(r, streamDecodeBufferBytes),
		txCount: txCount,
	}

	coinbase, err := bsvutil.NewTxFromReader(s.r)
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to decode the coinbase transaction", err)
	}

	s.coinbase = coinbase
	s.txBytes = int64(coinbase.MsgTx().SerializeSize())

	return s, nil
}

// Coinbase returns the block's first transaction, decoded by newStreamTxSource.
func (s *streamTxSource) Coinbase() *bsvutil.Tx {
	return s.coinbase
}

func (s *streamTxSource) Next() (*bsvutil.Tx, error) {
	if s.yielded >= s.txCount {
		return nil, io.EOF
	}

	s.yielded++

	if s.yielded == 1 {
		coinbase := s.coinbase
		// Drop our own reference: the pipeline owns it from here, and the
		// source must not pin a transaction the pipeline may have finished
		// with.
		s.coinbase = nil

		return coinbase, nil
	}

	tx, err := bsvutil.NewTxFromReader(s.r)
	if err != nil {
		return nil, errors.NewBlockInvalidError("failed to decode transaction %d of %d", s.yielded, s.txCount, err)
	}

	s.txBytes += int64(tx.MsgTx().SerializeSize())

	return tx, nil
}

// blockSize reconstructs the block's serialized size from the parts the
// streaming entry sees: the 80 byte header, the transaction-count varint the
// transport already consumed, and every transaction decoded so far. It matches
// wire.MsgBlock.SerializeSize, which is what the buffered entry passes to
// model.NewBlock.
func (s *streamTxSource) blockSize(headerLen int) int64 {
	return int64(headerLen) + int64(wire.VarIntSerializeSize(s.txCount)) + s.txBytes
}

// ProgressReader wraps the transport's transaction stream so a watcher on
// another goroutine can see how far an ingest has got. A large block takes
// minutes to ingest while the peer stays silent on the socket, and the peer
// idle timer only advances on stream events — so without a progress signal the
// protocol layer cannot tell a slow-but-progressing ingest from a peer that
// went silent mid-payload.
//
// The protocol layer composes it: wrap the stream's TxReader, hand the wrapper
// to IngestBlock, and poll BytesRead/LastProgress from the peer's idle-timer
// goroutine. Wiring that poll into the idle timer is the next task's work;
// this type is the surface it wires to.
//
// IMPORTANT for that wiring: BytesRead can sit at 0 for a long time without
// the peer being at fault. IngestBlock performs two LOCAL waits before it
// reads a single transaction byte — WaitForBlockAssemblyReady and
// waitForPreviousBlockMined — and both hold the socket while they retry
// against our own services. LastProgress is therefore stamped when the reader
// is constructed, so a watcher sees a live ingest from the start rather than a
// zero time it cannot distinguish from a silent peer. A stalled LastProgress
// during that window means our own pipeline is slow, not the peer, and the
// peer must NOT be rotated for it.
//
// Close is forwarded to the wrapped stream when it has one, so the wrapper
// does not hide the transport's Closer from IngestBlock's release path.
type ProgressReader struct {
	r io.Reader

	bytesRead atomic.Uint64
	// lastProgress holds a UnixNano stamp, seeded at construction so it is
	// never zero for a reader an ingest is actually holding.
	lastProgress atomic.Int64
}

// NewProgressReader wraps r with a monotonic byte counter and a last-progress
// stamp, both safe to read from any goroutine. The stamp starts at the time of
// construction: the caller builds the reader immediately before handing it to
// IngestBlock, so that is the moment the ingest began.
func NewProgressReader(r io.Reader) *ProgressReader {
	p := &ProgressReader{r: r}
	p.lastProgress.Store(time.Now().UnixNano())

	return p
}

func (p *ProgressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.bytesRead.Add(uint64(n)) //nolint:gosec // n is non-negative
		p.lastProgress.Store(time.Now().UnixNano())
	}

	return n, err
}

// BytesRead reports how many payload bytes the ingest has taken off the stream.
func (p *ProgressReader) BytesRead() uint64 {
	return p.bytesRead.Load()
}

// LastProgress reports when the ingest last took bytes off the stream, or the
// reader's construction time when no bytes have arrived yet. It is never the
// zero time, so a watcher can always compute an age. Pair it with BytesRead to
// tell the two silent states apart: BytesRead of 0 with a fresh stamp is an
// ingest still in its local pre-read waits, not a silent peer.
func (p *ProgressReader) LastProgress() time.Time {
	return time.Unix(0, p.lastProgress.Load())
}

// Close releases the wrapped stream when it owns one.
func (p *ProgressReader) Close() error {
	return closeIngestStream(p.r)
}

// closeIngestStream releases a transaction stream that owns a Closer.
//
// The transport's BlockStream.TxReader is exactly that: its Close forwards to
// BlockStream.Close, which drains whatever the consumer did not read so the
// connection stays aligned on the next message header, and which frees the
// read loop that stays parked for the whole ingest. Releasing it is not
// optional — the peer delivers nothing else until it happens.
//
// A plain reader with no Closer (tests, replayed fixtures) is a no-op.
func closeIngestStream(r io.Reader) error {
	if closer, ok := r.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

// PreAdmit performs the two blockchain lookups IngestBlock makes before it
// reads a payload byte, so a caller can run them under a deadline it controls.
// It touches no stream and reserves nothing.
//
// A missing parent is an answer (ParentMissing), not an error: it is the
// expected out-of-order case, and it is reported so the caller can skip a
// pipeline run that would fail on it.
func (sm *svp2pBridge) PreAdmit(ctx context.Context, header *wire.BlockHeader) (PreAdmitResult, error) {
	if header == nil {
		return PreAdmitResult{}, errors.NewProcessingError("[PreAdmit] block message carries no header")
	}

	blockHash := header.BlockHash()

	exists, err := sm.blockchainClient.GetBlockExists(ctx, &blockHash)
	if err != nil {
		return PreAdmitResult{}, errors.NewProcessingError("[PreAdmit][%s] failed to check if block exists", blockHash.String(), err)
	}

	if exists {
		return PreAdmitResult{Exists: true}, nil
	}

	if _, _, err = sm.blockchainClient.GetBlockHeader(ctx, &header.PrevBlock); err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			sm.logger.Debugf("[PreAdmit][%s] previous block %s not found (out-of-order; the block is not admitted)", blockHash.String(), header.PrevBlock)

			return PreAdmitResult{ParentMissing: true}, nil
		}

		return PreAdmitResult{}, errors.NewProcessingError("[PreAdmit][%s] failed to get the header of previous block %s", blockHash.String(), header.PrevBlock, err)
	}

	return PreAdmitResult{}, nil
}

// IngestBlock runs a streamed block through the relocated ingestion pipeline.
// It is HandleBlockDirect's contract with the wire block replaced by the
// transport's envelope: the same existence check, the same parent-height
// validation, the same block-assembly readiness wait, and then the same
// pipeline — createTxMap, prepareSubtrees, extendTransactions, createUtxos,
// createSubtrees, writeSubtree, ProcessBlock — with the transaction map fed by
// a single streaming pass instead of a decoded wire.MsgBlock.
//
// peerAddr is used for logging only. No peer-liveness state lives in bridge;
// the transport and protocol layers own the idle timer, and ProgressReader is
// how they observe a long ingest.
//
// The transaction stream is released on every exit path, including every error
// path: the transport's read loop stays parked until it closes.
func (sm *svp2pBridge) IngestBlock(ctx context.Context, header *wire.BlockHeader, txCount uint64, txReader io.Reader, peerAddr string) (err error) {
	defer func() {
		if closeErr := closeIngestStream(txReader); closeErr != nil && err == nil {
			err = errors.NewProcessingError("[IngestBlock] failed to release the block stream", closeErr)
		}
	}()

	if header == nil {
		return errors.NewProcessingError("[IngestBlock] block message carries no header")
	}

	if txCount == 0 {
		return errors.NewBlockInvalidError("[IngestBlock] block declares no transactions")
	}

	txCountInt, err := safeconversion.Uint64ToInt(txCount)
	if err != nil {
		return errors.NewBlockInvalidError("[IngestBlock] transaction count %d out of range", txCount, err)
	}

	blockHash := header.BlockHash()

	sm.logger.Debugf("[IngestBlock][%s] starting handling block", blockHash.String())

	// Check whether this block already exists. The stream is still released by
	// the deferred close above, which drains the payload so the connection
	// stays aligned.
	blockExists, err := sm.blockchainClient.GetBlockExists(ctx, &blockHash)
	if err != nil {
		sm.logger.Errorf("[IngestBlock][%s] failed to check if block exists: %s", blockHash.String(), err)
		return errors.NewProcessingError("failed to check if block exists", err)
	}

	if blockExists {
		sm.logger.Warnf("[IngestBlock][%s] block already exists", blockHash.String())
		return nil
	}

	// Look up the parent's height. A missing parent is an expected, recoverable
	// condition — a normal orphan or out-of-order tip announce, or a descendant
	// of a block rejected upstream — so it is logged at debug; genuine lookup
	// failures still log at error.
	_, previousBlockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, &header.PrevBlock)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			sm.logger.Debugf("[IngestBlock][%s] previous block %s not found (orphan/out-of-order; caller will request missing blocks): %v", blockHash.String(), header.PrevBlock, err)
		} else {
			sm.logger.Errorf("[IngestBlock][%s] failed to get block header for previous block %s: %s", blockHash.String(), header.PrevBlock, err)
		}

		return errors.NewProcessingError("failed to get block header for previous block %s", header.PrevBlock, err)
	}

	// A wire block header carries no height, so the height always comes from
	// the parent. The buffered entry's "height already set" branch has no
	// streaming counterpart: it exists only for a bsvutil.Block whose height
	// some earlier caller stamped.
	blockHeight := previousBlockHeaderMeta.Height + 1

	blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeight)
	if err != nil {
		return errors.NewProcessingError("failed to convert block height to int32", err)
	}

	ctx, _, deferFn := tracing.Tracer("svp2pBridge").Start(ctx, "IngestBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[IngestBlock][%s %d] %d txs, peer %s",
			blockHash.String(),
			blockHeight,
			txCount,
			peerAddr,
		),
		tracing.WithTag("blockHash", blockHash.String()),
		tracing.WithTag("peer", peerAddr),
		// Shares the buffered entry's histogram: only one of the two entries
		// is reachable in a running node — protocol calls IngestBlock and
		// nothing calls HandleBlockDirect — so the series stays unambiguous.
		// Re-checked at the end of Phase 3 and kept; the decision, what it
		// depends on and what breaks it are recorded on metrics.go's own
		// header comment.
		tracing.WithHistogram(prometheusSvp2pBridgeHandleBlockDirect),
	)

	defer func() {
		prometheusSvp2pBridgeBlockHeight.Set(float64(blockHeight))

		deferFn(err)
	}()

	// Wait for block assembly to be ready.
	if waitErr := blockassemblyutil.WaitForBlockAssemblyReady(ctx, sm.logger, sm.blockAssembly, blockHeight, sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly); waitErr != nil {
		err = blockAssemblyNotReady(waitErr)
		return err
	}

	// Wait for the previous block's setTxMined to complete before validating
	// this block's transactions, so BIP68 sequence lock validation can look up
	// parent transaction block heights. Skipped on the below-checkpoint
	// outpoint-only fast path.
	if sm.needsParentMinedWait(blockHeight) {
		if err = sm.waitForPreviousBlockMined(ctx, &header.PrevBlock, blockHeight); err != nil {
			return err
		}
	}

	var headerBytes bytes.Buffer
	if err = header.Serialize(&headerBytes); err != nil {
		return errors.NewProcessingError("failed to serialize header", err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create block header from bytes", err)
	}

	// Open the stream. This decodes the coinbase and nothing else; every other
	// transaction is decoded on demand by the pipeline below.
	source, err := newStreamTxSource(txReader, txCount)
	if err != nil {
		return err
	}

	var coinbase bytes.Buffer
	if err = source.Coinbase().MsgTx().Serialize(&coinbase); err != nil {
		return errors.NewProcessingError("failed to serialize coinbase", err)
	}

	// Single coinbase decode per block, retained into the teranodeBlock model
	// for downstream use.
	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	sb := sourceBlock{
		hash:      blockHash,
		prevBlock: header.PrevBlock,
		height:    blockHeightInt32,
		timestamp: header.Timestamp,
		txCount:   txCountInt,
	}

	// Validate all subtrees and store all subtree data; this also spends and
	// creates all UTXOs. The source is fully consumed by the time it returns,
	// so the block's serialized size is known immediately afterwards.
	subtrees, preparedSubtreeSlices, blockID, err := sm.prepareSubtreesFromSource(ctx, sb, source)
	if err != nil {
		return err
	}

	blockSize := source.blockSize(headerBytes.Len())
	if blockSize <= 0 {
		return errors.NewProcessingError("[IngestBlock][%s] computed a non-positive block size %d", blockHash.String(), blockSize)
	}

	blockSizeUint64 := uint64(blockSize)

	teranodeBlock, err := model.NewBlock(modelHeader, coinbaseTx, subtrees, txCount, blockSizeUint64, blockHeight, blockID)
	if err != nil {
		return errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// Pre-check that there is enough proof of work on the block, before any
	// other processing.
	headerValid, _, err := teranodeBlock.Header.HasMetTargetDifficulty()
	if !headerValid {
		return errors.NewBlockInvalidError("invalid block header: %s", teranodeBlock.Header.Hash().String(), err)
	}

	// Unified route integrity floor: block.Valid (and its CheckMerkleRoot) does
	// not run server-side on this route, and the subtrees were built here from
	// the streamed transactions. Verify the merkle root so a corrupt or
	// tampered payload can never reach the UTXO store. The CVE-2012-2459
	// duplicate-transaction check is a second, independent floor: that mutation
	// preserves the merkle root via the duplicate-last-when-odd rule, so
	// CheckMerkleRoot alone would pass it.
	if preparedSubtreeSlices != nil {
		teranodeBlock.SubtreeSlices = preparedSubtreeSlices

		if err = teranodeBlock.CheckMerkleRoot(ctx); err != nil {
			return errors.NewBlockInvalidError("[IngestBlock][%s %d] merkle root mismatch on unified route", blockHash.String(), blockHeight, err)
		}

		if err = model.CheckSubtreeSlicesForDuplicateTxs(preparedSubtreeSlices); err != nil {
			return errors.NewBlockInvalidError("[IngestBlock][%s %d] duplicate transaction on unified route", blockHash.String(), blockHeight, err)
		}

		teranodeBlock.SubtreeSlices = nil
	}

	if err = sm.ProcessBlock(ctx, teranodeBlock); err != nil {
		return err
	}

	// Do not request rejected transactions again until a new block has been
	// processed: a full wipe, not a per-tx removal, matching legacy's own
	// clearing site exactly (netsync/manager.go:1855, sm.rejectedTxns.Clear(),
	// immediately after its "accepted block %v at height %d" log at :1852).
	// See ingest_tx.go's IngestTx for the set this clears. The nil check is
	// for callers that construct svp2pBridge directly rather than through
	// New() (this package's own test harnesses do); every production path
	// goes through New(), which always initializes the set.
	if sm.rejectedTxns != nil {
		sm.rejectedTxns.Clear()
	}

	return nil
}

// blockAssemblyNotReady re-codes the block-assembly readiness gate's failure as
// a ServiceUnavailable error, which is what it is: our own block assembly is
// behind, and the peer that delivered the block had nothing to do with it.
//
// WaitForBlockAssemblyReady (util/blockassemblyutil/blockassembly_wait.go)
// exhausts retry.WithRetryCount(100) and returns a ProcessingError reading
// "block assembly is behind, block height %d, block assembly height %d".
// ERR_PROCESSING is NOT in errors.IsTransientLocalError's set
// (errors/error_utils.go:68-77, {ErrServiceError, ErrStorageError,
// ErrServiceUnavailable, ErrStorageUnavailable}), so under legacy's
// shouldDisconnectOnBlockErr rule (services/legacy/peer_server.go:1203-1209,
// !IsTransientLocalError) that exhaustion read as a peer fault and cost an
// HONEST peer its connection.
//
// Re-coding here rather than string-matching at the classification seam keeps
// the decision structural. ErrServiceUnavailable is the same code legacy's own
// per-block backoff throttle uses (peer_server.go:1196-1199, "#1187"), so the
// admission failure backoff (Admission.RecordFailure) bounds the retry cost
// exactly as it does for every other local fault.
//
// The original error is wrapped, not discarded: the operator still sees which
// heights were involved.
func blockAssemblyNotReady(err error) error {
	return errors.NewServiceUnavailableError("[svp2p] block assembly is not ready for this block", err)
}
