package muhash

import (
	"crypto/sha256"
	"math/big"

	"golang.org/x/crypto/chacha20"
)

// numBytes is the byte length of a 3072-bit group element (3072 / 8).
const numBytes = 384

// modulus is the 3072-bit prime M = 2^3072 - 1103717. The accumulator operates
// in the multiplicative group of integers modulo M.
var modulus = func() *big.Int {
	m := new(big.Int).Lsh(big.NewInt(1), 3072)
	return m.Sub(m, big.NewInt(1103717))
}()

// mulMod returns (a * b) mod modulus.
func mulMod(a, b *big.Int) *big.Int {
	r := new(big.Int).Mul(a, b)
	return r.Mod(r, modulus)
}

// numToBytes serializes x as a fixed-width little-endian byte slice of numBytes.
func numToBytes(x *big.Int) []byte {
	be := x.FillBytes(make([]byte, numBytes)) // big-endian, left zero-padded
	le := make([]byte, numBytes)
	for i := range numBytes {
		le[i] = be[numBytes-1-i]
	}
	return le
}

// elementKeystream writes the frozen numBytes-long element keystream for data
// into dst (which must be exactly numBytes): key = SHA256(data); ChaCha20
// keystream under that key with an all-zero 12-byte nonce and counter 0. dst is
// zeroed first, so its prior contents are irrelevant. This construction is
// frozen — changing it changes every commitment. It is the single source of the
// element bytes for both elementToNum and the hot-path fold.
func elementKeystream(dst, data []byte) {
	key := sha256.Sum256(data)

	var nonce [12]byte

	c, err := chacha20.NewUnauthenticatedCipher(key[:], nonce[:])
	if err != nil {
		// key is always 32 bytes and nonce 12 bytes, so this cannot happen.
		panic(err)
	}

	for i := range dst {
		dst[i] = 0
	}

	c.XORKeyStream(dst, dst) // dst is zero-filled, so output is the raw keystream
}

// leToNum sets out to the little-endian integer in le reduced mod modulus,
// using beScratch (len(le)) as reversal space. le and beScratch must not alias.
func leToNum(out *big.Int, le, beScratch []byte) {
	for i := range le {
		beScratch[len(le)-1-i] = le[i]
	}

	out.SetBytes(beScratch)
	out.Mod(out, modulus)
}

// elementToNum maps arbitrary data to a group element in [0, modulus). It
// allocates; the set-wide hot path uses reused scratch instead (see mulElement).
func elementToNum(data []byte) *big.Int {
	buf := make([]byte, numBytes)
	elementKeystream(buf, data)

	return bytesToNum(buf)
}

// bytesToNum interprets a little-endian byte slice as an integer reduced mod modulus.
func bytesToNum(le []byte) *big.Int {
	out := new(big.Int)
	leToNum(out, le, make([]byte, len(le)))

	return out
}

// sha256Sum is a thin wrapper so muhash.go need not import crypto/sha256 directly.
func sha256Sum(b []byte) [32]byte {
	return sha256.Sum256(b)
}
