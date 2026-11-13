package repository

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"math"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

const (
	subtreeStreamHeaderSize = chainhash.HashSize + 24
	subtreeNodeRecordSize   = chainhash.HashSize + 16
	subtreeStreamBufferSize = 32 * 1024
	subtreePageMaxNodes     = 100 // Keep in sync with HTTP pagination bounds.

	errUnableToReadSubtreeNode = "unable to read subtree node information"
)

type subtreeStreamHeader struct {
	rootHash    chainhash.Hash
	fees        uint64
	sizeInBytes uint64
	numLeaves   uint64
}

func readSubtreeStreamHeader(buf io.Reader) (subtreeStreamHeader, error) {
	var byteBuffer [subtreeStreamHeaderSize]byte
	if _, err := io.ReadFull(buf, byteBuffer[:]); err != nil {
		return subtreeStreamHeader{}, errors.NewProcessingError("unable to read subtree root information", err)
	}

	return subtreeStreamHeader{
		rootHash:    chainhash.Hash(byteBuffer[:chainhash.HashSize]),
		fees:        binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize : chainhash.HashSize+8]),
		sizeInBytes: binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize+8 : chainhash.HashSize+16]),
		numLeaves:   binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize+16 : chainhash.HashSize+24]),
	}, nil
}

func readSubtreeNodeRecord(buf *bufio.Reader) ([subtreeNodeRecordSize]byte, error) {
	var byteBuffer [subtreeNodeRecordSize]byte
	if _, err := io.ReadFull(buf, byteBuffer[:]); err != nil {
		return byteBuffer, errors.NewProcessingError(errUnableToReadSubtreeNode, err)
	}

	return byteBuffer, nil
}

func subtreeNodeFromRecord(byteBuffer [subtreeNodeRecordSize]byte) subtreepkg.Node {
	return subtreepkg.Node{
		Hash:        chainhash.Hash(byteBuffer[:chainhash.HashSize]),
		Fee:         binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize : chainhash.HashSize+8]),
		SizeInBytes: binary.LittleEndian.Uint64(byteBuffer[chainhash.HashSize+8 : chainhash.HashSize+16]),
	}
}

func readSubtreeHashChunk(ctx context.Context, buf *bufio.Reader, chunkSize int) ([]chainhash.Hash, error) {
	chunkHashes := make([]chainhash.Hash, chunkSize)
	for i := 0; i < chunkSize; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		byteBuffer, err := readSubtreeNodeRecord(buf)
		if err != nil {
			return nil, err
		}

		chunkHashes[i] = chainhash.Hash(byteBuffer[:chainhash.HashSize])
	}

	return chunkHashes, nil
}

// skipToPageOffset positions the stream at the first record of the requested
// page. Reading and discarding every preceding 48-byte record turned a one-row
// request near the end of a million-node subtree into a full sequential scan, so
// seek past them when the store reader supports it — the local file store does,
// through its semaphoreReadCloser. Stores that hand back a non-seekable reader
// (memory, HTTP) fall back to a bulk bufio discard, which still avoids a
// per-record read call.
//
// The header must already have been consumed directly from reader, not through a
// bufio wrapper, or the seek would be relative to the wrong position. That is why
// the buffered reader is created here rather than by the caller.
func skipToPageOffset(ctx context.Context, reader io.Reader, offset int) (*bufio.Reader, error) {
	if offset > 0 {
		if seeker, ok := reader.(io.Seeker); ok {
			if _, err := seeker.Seek(int64(offset)*subtreeNodeRecordSize, io.SeekCurrent); err == nil {
				offset = 0
			}
		}
	}

	buf := bufio.NewReaderSize(reader, subtreeStreamBufferSize)

	remaining := offset * subtreeNodeRecordSize
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		chunk := remaining
		if chunk > subtreeStreamBufferSize {
			chunk = subtreeStreamBufferSize
		}

		discarded, err := buf.Discard(chunk)
		if err != nil {
			return nil, errors.NewProcessingError(errUnableToReadSubtreeNode, err)
		}

		remaining -= discarded
	}

	return buf, nil
}

func readSubtreeNodesPageFromReader(ctx context.Context, reader io.Reader, offset, limit int) ([]subtreepkg.Node, int, error) {
	if offset < 0 {
		return nil, 0, errors.NewInvalidArgumentError("offset cannot be negative: %d", offset)
	}
	if limit < 0 {
		return nil, 0, errors.NewInvalidArgumentError("limit cannot be negative: %d", limit)
	}
	if limit > subtreePageMaxNodes {
		limit = subtreePageMaxNodes
	}

	header, err := readSubtreeStreamHeader(reader)
	if err != nil {
		return nil, 0, err
	}

	totalNodes, err := safeconversion.Uint64ToInt(header.numLeaves)
	if err != nil {
		return nil, 0, errors.NewProcessingError("unable to convert subtree node count", err)
	}

	if limit == 0 || offset >= totalNodes {
		return []subtreepkg.Node{}, totalNodes, nil
	}

	buf, err := skipToPageOffset(ctx, reader, offset)
	if err != nil {
		return nil, 0, err
	}

	pageSize := limit
	if pageSize > totalNodes-offset {
		pageSize = totalNodes - offset
	}
	nodes := make([]subtreepkg.Node, 0, pageSize)
	for i := 0; i < pageSize; i++ {
		if err = ctx.Err(); err != nil {
			return nil, 0, err
		}

		byteBuffer, err := readSubtreeNodeRecord(buf)
		if err != nil {
			return nil, 0, err
		}

		nodes = append(nodes, subtreeNodeFromRecord(byteBuffer))
	}

	return nodes, totalNodes, nil
}

func readSubtreePageFromReader(ctx context.Context, reader io.Reader, offset, limit int) (*subtreepkg.Subtree, int, int, error) {
	if offset < 0 {
		return nil, 0, 0, errors.NewInvalidArgumentError("offset cannot be negative: %d", offset)
	}
	if limit < 0 {
		return nil, 0, 0, errors.NewInvalidArgumentError("limit cannot be negative: %d", limit)
	}
	if limit > subtreePageMaxNodes {
		limit = subtreePageMaxNodes
	}

	header, err := readSubtreeStreamHeader(reader)
	if err != nil {
		return nil, 0, 0, err
	}

	totalNodes, err := safeconversion.Uint64ToInt(header.numLeaves)
	if err != nil {
		return nil, 0, 0, errors.NewProcessingError("unable to convert subtree node count", err)
	}

	height := 0
	if totalNodes > 0 {
		height = int(math.Ceil(math.Log2(float64(totalNodes))))
	}

	if limit == 0 || offset >= totalNodes {
		return &subtreepkg.Subtree{
			Height:           height,
			Fees:             header.fees,
			SizeInBytes:      header.sizeInBytes,
			Nodes:            []subtreepkg.Node{},
			ConflictingNodes: []chainhash.Hash{},
		}, offset, totalNodes, nil
	}

	pageSize := limit
	if pageSize > totalNodes-offset {
		pageSize = totalNodes - offset
	}

	buf, err := skipToPageOffset(ctx, reader, offset)
	if err != nil {
		return nil, 0, 0, err
	}

	nodes := make([]subtreepkg.Node, 0, pageSize)
	for i := 0; i < pageSize; i++ {
		if err = ctx.Err(); err != nil {
			return nil, 0, 0, err
		}

		byteBuffer, err := readSubtreeNodeRecord(buf)
		if err != nil {
			return nil, 0, 0, err
		}

		nodes = append(nodes, subtreeNodeFromRecord(byteBuffer))
	}

	pageCoversFullSubtree := offset == 0 && pageSize == totalNodes
	conflictingNodes := []chainhash.Hash{}
	// Conflicting nodes are serialized after all node records, so reading them
	// for partial pages would scan the rest of large subtrees.
	if pageCoversFullSubtree {
		conflictingNodes, err = readSubtreeConflictingNodes(ctx, buf, totalNodes)
		if err != nil {
			return nil, 0, 0, err
		}
	}

	return &subtreepkg.Subtree{
		Height:           height,
		Fees:             header.fees,
		SizeInBytes:      header.sizeInBytes,
		Nodes:            nodes,
		ConflictingNodes: conflictingNodes,
	}, offset, totalNodes, nil
}

func readSubtreeConflictingNodes(ctx context.Context, buf *bufio.Reader, maxConflictingNodes int) ([]chainhash.Hash, error) {
	var bytes8 [8]byte
	if _, err := io.ReadFull(buf, bytes8[:]); err != nil {
		return nil, errors.NewProcessingError("unable to read number of conflicting nodes", err)
	}

	numConflictingLeaves, err := safeconversion.Uint64ToInt(binary.LittleEndian.Uint64(bytes8[:]))
	if err != nil {
		return nil, errors.NewProcessingError("unable to convert conflicting node count", err)
	}

	if maxConflictingNodes > subtreePageMaxNodes {
		maxConflictingNodes = subtreePageMaxNodes
	}
	if numConflictingLeaves > maxConflictingNodes {
		return nil, errors.NewProcessingError("conflicting node count exceeds node count: %d > %d", numConflictingLeaves, maxConflictingNodes)
	}

	conflictingNodes := make([]chainhash.Hash, numConflictingLeaves)
	for i := 0; i < numConflictingLeaves; i++ {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if _, err = io.ReadFull(buf, conflictingNodes[i][:]); err != nil {
			return nil, errors.NewProcessingError("unable to read conflicting node %d", i, err)
		}
	}

	return conflictingNodes, nil
}

type subtreeNodeHashesReadCloser struct {
	ctx       context.Context
	source    io.ReadCloser
	buf       *bufio.Reader
	remaining uint64
	pending   []byte
	nodeBytes [subtreeNodeRecordSize]byte
}

func newSubtreeNodeHashesReadCloser(ctx context.Context, source io.ReadCloser) (io.ReadCloser, error) {
	buf := bufio.NewReaderSize(source, subtreeStreamBufferSize)
	header, err := readSubtreeStreamHeader(buf)
	if err != nil {
		_ = source.Close()
		return nil, err
	}

	return &subtreeNodeHashesReadCloser{
		ctx:       ctx,
		source:    source,
		buf:       buf,
		remaining: header.numLeaves,
	}, nil
}

func (r *subtreeNodeHashesReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var total int
	for total < len(p) {
		if len(r.pending) == 0 {
			if r.remaining == 0 {
				if total > 0 {
					return total, nil
				}
				return 0, io.EOF
			}

			if err := r.ctx.Err(); err != nil {
				if total > 0 {
					return total, nil
				}
				return 0, err
			}

			if _, err := io.ReadFull(r.buf, r.nodeBytes[:]); err != nil {
				if total > 0 {
					return total, errors.NewProcessingError(errUnableToReadSubtreeNode, err)
				}
				return 0, errors.NewProcessingError(errUnableToReadSubtreeNode, err)
			}

			r.pending = r.nodeBytes[:chainhash.HashSize]
			r.remaining--
		}

		n := copy(p[total:], r.pending)
		total += n
		r.pending = r.pending[n:]
	}

	return total, nil
}

func (r *subtreeNodeHashesReadCloser) Close() error {
	return r.source.Close()
}

// GetSubtreeNodeHashesReader streams the node hashes of a subtree. The response is
// paced by the client, so the store reader behind it stays open for as long as the
// consumer takes — the same shape as GetSubtreeDataReader. Take a stream permit for
// that whole lifetime when the operator has configured one; the default of 0 keeps
// today's unlimited behaviour so peer catchup, which runs many concurrent unsigned
// fetches of this route, is unaffected unless an operator opts in.
func (repo *Repository) GetSubtreeNodeHashesReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error) {
	if err := acquireSemaphorePermit(ctx, repo.semSubtreeStream, "GetSubtreeNodeHashesReader"); err != nil {
		return nil, err
	}

	reader, err := repo.GetSubtreeTxIDsReader(ctx, hash)
	if err != nil {
		releaseSemaphorePermit(repo.semSubtreeStream)
		return nil, err
	}

	nodeHashesReader, err := newSubtreeNodeHashesReadCloser(ctx, reader)
	if err != nil {
		releaseSemaphorePermit(repo.semSubtreeStream)
		return nil, err
	}

	return &semaphoreReadCloser{
		ReadCloser: nodeHashesReader,
		sem:        repo.semSubtreeStream,
	}, nil
}

func (repo *Repository) GetSubtreeNodesPage(ctx context.Context, hash *chainhash.Hash, offset, limit int) ([]subtreepkg.Node, int, error) {
	reader, err := repo.GetSubtreeTxIDsReader(ctx, hash)
	if err != nil {
		return nil, 0, err
	}
	defer reader.Close()

	return readSubtreeNodesPageFromReader(ctx, reader, offset, limit)
}

func (repo *Repository) GetSubtreePage(ctx context.Context, hash *chainhash.Hash, offset, limit int) (*subtreepkg.Subtree, int, int, error) {
	reader, err := repo.GetSubtreeTxIDsReader(ctx, hash)
	if err != nil {
		return nil, 0, 0, err
	}
	defer reader.Close()

	return readSubtreePageFromReader(ctx, reader, offset, limit)
}
