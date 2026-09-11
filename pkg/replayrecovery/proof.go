package replayrecovery

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Inclusion contains independently checkable transaction inclusion evidence.
// Header is the exact 80-byte Bitcoin header; branches use display hash order.
type Inclusion struct {
	RawTx        string   `json:"raw_tx"`
	Header       []byte   `json:"header"`
	Height       uint32   `json:"height"`
	MerkleBranch []string `json:"merkle_branch"`
	Index        uint32   `json:"index"`
}

// VerifyInclusion authenticates the branch against the exact header bytes.
// Canonical chain membership must be established separately at the pinned tip.
func VerifyInclusion(id string, p Inclusion) (string, error) {
	h, err := strictHash(id)
	if err != nil {
		return "", err
	}
	if len(p.Header) != 80 || len(p.MerkleBranch) > 32 {
		return "", fmt.Errorf("invalid inclusion header or branch length")
	}
	index := p.Index
	for _, s := range p.MerkleBranch {
		other, e := strictHash(s)
		if e != nil {
			return "", e
		}
		if index&1 == 0 {
			*h = hashPair(*h, *other)
		} else {
			*h = hashPair(*other, *h)
		}
		index >>= 1
	}
	if index != 0 || !bytes.Equal(h[:], p.Header[36:68]) {
		return "", fmt.Errorf("merkle root mismatch")
	}
	return chainhash.DoubleHashH(p.Header).String(), nil
}

func strictHash(s string) (*chainhash.Hash, error) {
	if len(s) != 64 {
		return nil, fmt.Errorf("invalid hash length")
	}
	h, e := chainhash.NewHashFromStr(s)
	if e != nil || h.String() != s {
		return nil, fmt.Errorf("invalid canonical hash")
	}
	return h, nil
}
func hashPair(a, b chainhash.Hash) chainhash.Hash {
	var pair [64]byte
	copy(pair[:32], a[:])
	copy(pair[32:], b[:])
	return chainhash.DoubleHashH(pair[:])
}

func compact(r *bytes.Reader) (uint64, error) {
	b, e := r.ReadByte()
	if e != nil {
		return 0, e
	}
	var n uint64
	switch b {
	case 253:
		var x uint16
		e = binary.Read(r, binary.LittleEndian, &x)
		n = uint64(x)
		if n < 253 {
			return 0, fmt.Errorf("noncanonical varint")
		}
	case 254:
		var x uint32
		e = binary.Read(r, binary.LittleEndian, &x)
		n = uint64(x)
		if n <= 65535 {
			return 0, fmt.Errorf("noncanonical varint")
		}
	case 255:
		e = binary.Read(r, binary.LittleEndian, &n)
		if n <= 4294967295 {
			return 0, fmt.Errorf("noncanonical varint")
		}
	default:
		n = uint64(b)
	}
	return n, e
}

// ReadBoundedTransaction validates wire counts and script lengths against bytes
// actually present before bt allocates scripts. It accepts standard or extended
// archive encoding and consumes exactly one transaction from the reader.
func ReadBoundedTransaction(r *bytes.Reader) (*bt.Tx, error) {
	start := r.Size() - int64(r.Len())
	skip := func(n uint64) error {
		if n > uint64(r.Len()) { // #nosec G115 -- bytes.Reader.Len is nonnegative.
			return fmt.Errorf("transaction field exceeds available bytes")
		}
		_, err := r.Seek(int64(n), io.SeekCurrent) // #nosec G115 -- n <= r.Len() <= MaxInt, therefore fits int64.
		return err
	}
	script := func() error {
		n, err := compact(r)
		if err != nil {
			return err
		}
		return skip(n)
	}
	if err := skip(4); err != nil {
		return nil, err
	}
	inputs, err := compact(r)
	if err != nil {
		return nil, err
	}
	extended := false
	if inputs == 0 {
		marker := make([]byte, 5)
		if _, err = io.ReadFull(r, marker); err != nil || !bytes.Equal(marker, []byte{0, 0, 0, 0, 239}) {
			return nil, fmt.Errorf("invalid transaction input count")
		}
		extended = true
		inputs, err = compact(r)
		if err != nil {
			return nil, err
		}
	}
	if inputs == 0 || inputs > uint64(r.Len()/41) { // #nosec G115 -- bytes.Reader.Len is nonnegative.
		return nil, fmt.Errorf("transaction input count exceeds available bytes")
	}
	for i := uint64(0); i < inputs; i++ {
		if err = skip(36); err != nil {
			return nil, err
		}
		if err = script(); err != nil {
			return nil, err
		}
		if err = skip(4); err != nil {
			return nil, err
		}
		if extended {
			if err = skip(8); err != nil {
				return nil, err
			}
			if err = script(); err != nil {
				return nil, err
			}
		}
	}
	outputs, err := compact(r)
	if err != nil {
		return nil, err
	}
	if outputs == 0 || outputs > uint64(r.Len()/9) { // #nosec G115 -- bytes.Reader.Len is nonnegative.
		return nil, fmt.Errorf("transaction output count exceeds available bytes")
	}
	for i := uint64(0); i < outputs; i++ {
		if err = skip(8); err != nil {
			return nil, err
		}
		if err = script(); err != nil {
			return nil, err
		}
	}
	if err = skip(4); err != nil {
		return nil, err
	}
	end := r.Size() - int64(r.Len())
	if _, err = r.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	tx := &bt.Tx{}
	n, err := tx.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	if n != end-start {
		return nil, fmt.Errorf("transaction encoding length mismatch")
	}
	return tx, nil
}

// ReadBoundedTransactionStream consumes exactly one archive transaction. Wire
// lengths are checked before reading or allocating scripts, with bounded work.
func ReadBoundedTransactionStream(r io.Reader, maxBytes int64) (*bt.Tx, error) {
	if maxBytes < 10 || maxBytes > 256<<20 {
		return nil, fmt.Errorf("invalid transaction byte limit")
	}
	var wire bytes.Buffer
	take := func(n uint64) error {
		remaining := maxBytes - int64(wire.Len())
		if remaining < 0 || n > uint64(remaining) { // #nosec G115 -- negative remaining is rejected first.
			return fmt.Errorf("transaction exceeds byte limit")
		}
		_, err := io.CopyN(&wire, r, int64(n)) // #nosec G115 -- n is bounded above by maxBytes <= 256 MiB.
		return err
	}
	count := func() (uint64, error) {
		start := wire.Len()
		if err := take(1); err != nil {
			return 0, err
		}
		n := 0
		switch wire.Bytes()[start] {
		case 253:
			n = 2
		case 254:
			n = 4
		case 255:
			n = 8
		}
		if err := take(uint64(n)); err != nil {
			return 0, err
		}
		return compact(bytes.NewReader(wire.Bytes()[start:]))
	}
	script := func() error {
		n, err := count()
		if err != nil {
			return err
		}
		return take(n)
	}
	if err := take(4); err != nil {
		return nil, err
	}
	inputs, err := count()
	if err != nil {
		return nil, err
	}
	extended := false
	if inputs == 0 {
		start := wire.Len()
		if err = take(5); err != nil {
			return nil, err
		}
		if !bytes.Equal(wire.Bytes()[start:], []byte{0, 0, 0, 0, 239}) {
			return nil, fmt.Errorf("invalid extended marker")
		}
		extended = true
		inputs, err = count()
		if err != nil {
			return nil, err
		}
	}
	if inputs == 0 || inputs > uint64(maxBytes/41) || inputs > 1000000 {
		return nil, fmt.Errorf("transaction input count exceeds bound")
	}
	for i := uint64(0); i < inputs; i++ {
		if err = take(36); err != nil {
			return nil, err
		}
		if err = script(); err != nil {
			return nil, err
		}
		if err = take(4); err != nil {
			return nil, err
		}
		if extended {
			if err = take(8); err != nil {
				return nil, err
			}
			if err = script(); err != nil {
				return nil, err
			}
		}
	}
	outputs, err := count()
	if err != nil {
		return nil, err
	}
	if outputs == 0 || outputs > uint64(maxBytes/9) || outputs > 1000000 {
		return nil, fmt.Errorf("transaction output count exceeds bound")
	}
	for i := uint64(0); i < outputs; i++ {
		if err = take(8); err != nil {
			return nil, err
		}
		if err = script(); err != nil {
			return nil, err
		}
	}
	if err = take(4); err != nil {
		return nil, err
	}
	return ReadBoundedTransaction(bytes.NewReader(wire.Bytes()))
}
