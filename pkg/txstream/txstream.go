// Package txstream reads transactions from a byte stream and computes each one's id from the bytes
// as they pass, instead of serializing the parsed transaction again to hash it.
//
// It reads both of the forms teranode stores: the standard form a peer sends, and the extended form
// that also carries, for every input, the value and locking script of the output it spends. An id is
// defined over the standard form only, so the extended marker and each input's extension are read
// without being hashed.
//
// On mainnet on 2026-09-25 re-serializing to hash was 38 GB of every 141 GB the node allocated, all
// of it in block validation reading subtree data files back.
package txstream

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// extendedMarker follows the version in an extended transaction. go-bt reads it as an input count of
// zero, an output count of zero and a lock time whose big-endian value is 0xEF, and so does this.
var extendedMarker = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}

// maxScriptAlloc is the largest script this reader will allocate, the same bound go-bt applies.
const maxScriptAlloc = bt.MaxArenaAlloc

// DataOutputScript stands in for the locking script of an output whose script was passed through
// without being kept (see Options.SkipDataScripts). It is OP_FALSE OP_RETURN, so anything checking
// whether the output can be spent sees that it cannot. It is shared: never modify it.
var DataOutputScript = bscript.Script{bscript.OpFALSE, bscript.OpRETURN}

// BeforeOutputs is called once a transaction's version and inputs have been read, before any output.
// It may return a writer: every byte of the outputs and the lock time is then copied to it as it is
// read. Returning nil copies nothing.
type BeforeOutputs func(tx *bt.Tx) (io.Writer, error)

// Options control one Read.
type Options struct {
	// BeforeOutputs, when set, is called between the inputs and the outputs.
	BeforeOutputs BeforeOutputs
	// SkipDataScripts passes the script of every OP_FALSE OP_RETURN output through to the writer
	// BeforeOutputs returned without allocating it, and leaves DataOutputScript in its place. It only
	// applies while there is such a writer, since otherwise the bytes would be lost. Such an output
	// can never be spent, and at recent heights these scripts are most of a block's bytes.
	SkipDataScripts bool
}

// Reader reads transactions one after another from one stream.
type Reader struct {
	src     *bufio.Reader
	h       hash.Hash
	sum     [sha256.Size]byte
	hashing bool
	// size counts the bytes hashed, which is the transaction's standard size.
	size    int64
	out     io.Writer
	scratch [32 * 1024]byte
}

// NewReader reads from r. It reuses r when r is already a *bufio.Reader.
func NewReader(r io.Reader) *Reader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}

	return &Reader{src: br, h: sha256.New()}
}

// Read passes bytes from the source to the hasher while hashing is on, and to the output writer
// while there is one.
func (t *Reader) Read(p []byte) (int, error) {
	n, err := t.src.Read(p)
	if n > 0 {
		if t.hashing {
			t.h.Write(p[:n])
			t.size += int64(n)
		}

		if t.out != nil {
			if _, werr := t.out.Write(p[:n]); werr != nil {
				return n, werr
			}
		}
	}

	return n, err
}

// Next reads one transaction and returns it with its id and its standard size in bytes. The id is
// also set on the transaction, so TxIDChainHash answers without hashing again.
//
// A stream that ends cleanly before the first byte of a transaction returns io.EOF, and one that
// ends inside a transaction returns an error that is not io.EOF, as go-bt does.
func (t *Reader) Next(opts Options) (*bt.Tx, *chainhash.Hash, int64, error) {
	t.h.Reset()
	t.size = 0
	t.hashing = true
	t.out = nil

	defer func() {
		t.out = nil
	}()

	tx := &bt.Tx{}

	var version [4]byte
	if _, err := io.ReadFull(t, version[:]); err != nil {
		return nil, nil, 0, err
	}

	tx.Version = binary.LittleEndian.Uint32(version[:])

	extended := false
	if marker, err := t.src.Peek(len(extendedMarker)); err == nil && bytes.Equal(marker, extendedMarker) {
		if _, err = t.src.Discard(len(extendedMarker)); err != nil {
			return nil, nil, 0, unexpected(err)
		}

		extended = true
	}

	var inputCount bt.VarInt
	if _, err := inputCount.ReadFrom(t); err != nil {
		return nil, nil, 0, unexpected(err)
	}

	tx.Inputs = make([]*bt.Input, 0, min(uint64(inputCount), 1024))

	for i := uint64(0); i < uint64(inputCount); i++ {
		in, err := t.readInput(extended)
		if err != nil {
			return nil, nil, 0, err
		}

		tx.Inputs = append(tx.Inputs, in)
	}

	if extended {
		tx.SetExtended(true)
	}

	skipData := false

	if opts.BeforeOutputs != nil {
		w, err := opts.BeforeOutputs(tx)
		if err != nil {
			return nil, nil, 0, err
		}

		t.out = w
		skipData = w != nil && opts.SkipDataScripts
	}

	var outputCount bt.VarInt
	if _, err := outputCount.ReadFrom(t); err != nil {
		return nil, nil, 0, unexpected(err)
	}

	tx.Outputs = make([]*bt.Output, 0, min(uint64(outputCount), 1024))

	for i := uint64(0); i < uint64(outputCount); i++ {
		out, err := t.readOutput(skipData)
		if err != nil {
			return nil, nil, 0, err
		}

		tx.Outputs = append(tx.Outputs, out)
	}

	var lockTime [4]byte
	if _, err := io.ReadFull(t, lockTime[:]); err != nil {
		return nil, nil, 0, unexpected(err)
	}

	tx.LockTime = binary.LittleEndian.Uint32(lockTime[:])

	first := t.h.Sum(t.sum[:0])
	id := chainhash.Hash(sha256.Sum256(first))
	tx.SetTxHash(&id)

	return tx, &id, t.size, nil
}

func (t *Reader) readInput(extended bool) (*bt.Input, error) {
	var fixed [36]byte
	if _, err := io.ReadFull(t, fixed[:]); err != nil {
		return nil, unexpected(err)
	}

	prev, err := chainhash.NewHash(fixed[:32])
	if err != nil {
		return nil, err
	}

	in := &bt.Input{PreviousTxOutIndex: binary.LittleEndian.Uint32(fixed[32:])}
	if err = in.PreviousTxIDAdd(prev); err != nil {
		return nil, err
	}

	unlocking, err := t.readScript()
	if err != nil {
		return nil, err
	}

	in.UnlockingScript = bscript.NewFromBytes(unlocking)

	var sequence [4]byte
	if _, err = io.ReadFull(t, sequence[:]); err != nil {
		return nil, unexpected(err)
	}

	in.SequenceNumber = binary.LittleEndian.Uint32(sequence[:])

	if extended {
		// The spent output's value and script are not part of the id.
		t.hashing = false

		var satoshis [8]byte
		if _, err = io.ReadFull(t, satoshis[:]); err != nil {
			return nil, unexpected(err)
		}

		prevScript, err := t.readScript()
		if err != nil {
			return nil, err
		}

		t.hashing = true
		in.PreviousTxSatoshis = binary.LittleEndian.Uint64(satoshis[:])
		in.PreviousTxScript = bscript.NewFromBytes(prevScript)
	}

	return in, nil
}

func (t *Reader) readOutput(skipData bool) (*bt.Output, error) {
	var satoshis [8]byte
	if _, err := io.ReadFull(t, satoshis[:]); err != nil {
		return nil, unexpected(err)
	}

	out := &bt.Output{Satoshis: binary.LittleEndian.Uint64(satoshis[:])}

	if skipData {
		var l bt.VarInt
		if _, err := l.ReadFrom(t); err != nil {
			return nil, unexpected(err)
		}

		if l >= 2 {
			if head, err := t.src.Peek(2); err == nil && head[0] == bscript.OpFALSE && head[1] == bscript.OpRETURN {
				if err = t.pass(uint64(l)); err != nil {
					return nil, err
				}

				out.LockingScript = &DataOutputScript

				return out, nil
			}
		}

		script, err := t.readScriptBody(uint64(l))
		if err != nil {
			return nil, err
		}

		out.LockingScript = bscript.NewFromBytes(script)

		return out, nil
	}

	script, err := t.readScript()
	if err != nil {
		return nil, err
	}

	out.LockingScript = bscript.NewFromBytes(script)

	return out, nil
}

func (t *Reader) readScript() ([]byte, error) {
	var l bt.VarInt
	if _, err := l.ReadFrom(t); err != nil {
		return nil, unexpected(err)
	}

	return t.readScriptBody(uint64(l))
}

func (t *Reader) readScriptBody(l uint64) ([]byte, error) {
	if l > uint64(maxScriptAlloc) {
		return nil, errors.NewTxInvalidError("[txstream] script length %d exceeds %d", l, maxScriptAlloc)
	}

	script := make([]byte, l)
	if _, err := io.ReadFull(t, script); err != nil {
		return nil, unexpected(err)
	}

	return script, nil
}

// pass reads n bytes through the hasher and the writer without keeping them.
func (t *Reader) pass(n uint64) error {
	for n > 0 {
		chunk := t.scratch[:]
		if uint64(len(chunk)) > n {
			chunk = chunk[:n]
		}

		read, err := io.ReadFull(t, chunk)
		n -= uint64(read)

		if err != nil {
			return unexpected(err)
		}
	}

	return nil
}

// unexpected turns an end of stream inside a transaction into io.ErrUnexpectedEOF, so a caller
// looping until io.EOF cannot mistake a truncated transaction for a clean end.
func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}

	return err
}
