// Package muhash implements MuHash3072, a deterministic, order-independent
// (multiset) hash of a set of byte strings. Elements can be added and removed
// in any order; the Digest depends only on the multiset of current elements.
package muhash

import (
	"fmt"
	"math/big"
)

// MuHash3072 is a multiset hash over the group of integers modulo a 3072-bit
// prime. It is NOT safe for concurrent use; callers must synchronize access.
type MuHash3072 struct {
	numerator   *big.Int // product of added elements
	denominator *big.Int // product of removed elements

	// Scratch reused across Add/Remove so the set-wide hot path (one call per
	// UTXO over a 10^8-10^9 element set) does not allocate a group element,
	// keystream buffer, and reversal buffer per element. Safe to share because
	// MuHash3072 is single-threaded per the concurrency contract above.
	elem  *big.Int
	ksBuf []byte // element keystream (little-endian)
	beBuf []byte // big-endian reversal of ksBuf
}

// newAccumulator returns a MuHash3072 with numerator/denominator set to acc
// values of 1 and scratch buffers allocated.
func newAccumulator() *MuHash3072 {
	return &MuHash3072{
		numerator:   big.NewInt(1),
		denominator: big.NewInt(1),
		elem:        new(big.Int),
		ksBuf:       make([]byte, numBytes),
		beBuf:       make([]byte, numBytes),
	}
}

// New returns an accumulator representing the empty set.
func New() *MuHash3072 {
	return newAccumulator()
}

// mulElement folds data's group element into acc in place, reusing scratch.
func (m *MuHash3072) mulElement(acc *big.Int, data []byte) {
	elementKeystream(m.ksBuf, data)
	leToNum(m.elem, m.ksBuf, m.beBuf)

	acc.Mul(acc, m.elem)
	acc.Mod(acc, modulus)
}

// Add inserts data into the multiset.
func (m *MuHash3072) Add(data []byte) {
	m.mulElement(m.numerator, data)
}

// Remove deletes data from the multiset. Removing an element that was never
// added is well-defined (it becomes a denominator factor) and is exactly
// cancelled by a later Add of the same element.
func (m *MuHash3072) Remove(data []byte) {
	m.mulElement(m.denominator, data)
}

// Digest returns the 32-byte commitment: SHA256 of the little-endian encoding
// of numerator * denominator^-1 mod modulus.
func (m *MuHash3072) Digest() [32]byte {
	inv := new(big.Int).ModInverse(m.denominator, modulus)
	res := mulMod(m.numerator, inv)

	return sha256Sum(numToBytes(res))
}

// Bytes serializes the accumulator state as numerator||denominator, each a
// fixed numBytes little-endian integer (2*numBytes total).
func (m *MuHash3072) Bytes() []byte {
	out := make([]byte, 0, 2*numBytes)
	out = append(out, numToBytes(m.numerator)...)
	out = append(out, numToBytes(m.denominator)...)

	return out
}

// FromBytes restores an accumulator previously produced by Bytes.
func FromBytes(b []byte) (*MuHash3072, error) {
	if len(b) != 2*numBytes {
		return nil, fmt.Errorf("muhash: expected %d bytes, got %d", 2*numBytes, len(b))
	}

	m := newAccumulator()
	m.numerator = bytesToNum(b[:numBytes])
	m.denominator = bytesToNum(b[numBytes:])

	return m, nil
}
