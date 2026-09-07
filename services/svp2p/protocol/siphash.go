package protocol

import "encoding/binary"

// sipHash24 computes SipHash-2-4 (2 compression rounds, 4 finalization
// rounds) of data keyed with (k0, k1), ported from bitcoin-sv's CSipHasher
// (src/hash.h:253-275, src/hash.cpp:124-193). No dependency exists for this
// in the module (checked with `go list -m all | grep -i siphash`), so the
// ~80-line reference algorithm is carried here directly.
func sipHash24(k0, k1 uint64, data []byte) uint64 {
	// hash.cpp:124-129 (CSipHasher::CSipHasher): initial state XORs the
	// standard SipHash constants with the two key words.
	v0 := 0x736f6d6570736575 ^ k0
	v1 := 0x646f72616e646f6d ^ k1
	v2 := 0x6c7967656e657261 ^ k0
	v3 := 0x7465646279746573 ^ k1

	// hash.cpp:150-176 (CSipHasher::Write(data, size)): consume the input
	// eight bytes at a time, each block folded in with 2 SIPROUND passes.
	n := len(data)
	end := n - n%8

	for i := 0; i < end; i += 8 {
		m := binary.LittleEndian.Uint64(data[i : i+8])

		v3 ^= m
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
		v0 ^= m
	}

	// hash.cpp:178-193 (CSipHasher::Finalize): the trailing partial word is
	// packed into the low bytes, with the total length in the top byte.
	var last uint64
	for i := end; i < n; i++ {
		last |= uint64(data[i]) << (8 * uint(i-end))
	}
	last |= uint64(n) << 56

	v3 ^= last
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0 ^= last
	v2 ^= 0xff
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)
	v0, v1, v2, v3 = sipRound(v0, v1, v2, v3)

	return v0 ^ v1 ^ v2 ^ v3
}

// sipRound is one SipHash mixing round, the SIPROUND macro at
// bitcoin-sv src/hash.cpp:106-122.
func sipRound(v0, v1, v2, v3 uint64) (uint64, uint64, uint64, uint64) {
	v0 += v1
	v1 = rotl64(v1, 13)
	v1 ^= v0
	v0 = rotl64(v0, 32)
	v2 += v3
	v3 = rotl64(v3, 16)
	v3 ^= v2
	v0 += v3
	v3 = rotl64(v3, 21)
	v3 ^= v0
	v2 += v1
	v1 = rotl64(v1, 17)
	v1 ^= v2
	v2 = rotl64(v2, 32)

	return v0, v1, v2, v3
}

// rotl64 is the ROTL macro used by SIPROUND (bitcoin-sv src/hash.cpp:104).
func rotl64(x uint64, b uint) uint64 {
	return (x << b) | (x >> (64 - b))
}
