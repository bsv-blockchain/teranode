package txmetacache

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// This file defines the wire format used by the txmeta Kafka topic. These
// constants are the protocol contract between the producer (services/validator)
// and the consumers (services/subtreevalidation, services/legacy/netsync).
// New consumers must import these constants instead of redeclaring them so
// the producer and every consumer stay in lockstep.
//
// Two wire formats coexist on the topic, distinguished at the receiver by a
// multi-byte signature:
//
//	v1 (legacy)
//	  [4 bytes] entry count (uint32 LE)
//	  per entry: [32 hash][1 action][4 contentLen][N content]
//
//	v2 (partition-aware)
//	  [1 byte magic=0xFF][1 byte version=0x02][2 bytes uint16 LE BucketsCount][4 entry count LE]
//	  per entry: [8 xxhash][32 hash][1 action][4 contentLen][N content]
//
// v2 detection cannot rely on the magic byte alone: v1 messages with entry
// counts 255, 511, 767, ... have a little-endian low byte of 0xFF. Receivers
// MUST validate the full 4-byte header signature (magic + version + a
// plausible BucketsCount) AND check that the resulting v2 entry count is
// plausible for the buffer length; on any failure, fall back to v1 parsing.
//
// The two bytes after the version carry the producer's compile-time cache
// BucketsCount (uint16 LE; the production count 8192 fits with room to
// 65535). BucketsCount is build-tag selected and the v2 routing partitions
// by it, so a producer and a receiver compiled with different counts exchange
// valid messages forever while their bucket ranges quietly overlap - the
// partitioning degrades to lock contention, invisible to either side's
// metrics. On the wire, every receiver can say so. A mismatch is a loud
// signal, never a rejection: the records stay content-valid, only the
// partition-alignment promise is lost. A zero in these bytes is a batch
// still on the topic from a pre-buckets producer: parse it as v2, skip the
// comparison.

const (
	// WireActionADD signals that the entry's content carries a serialized
	// txmeta payload for the given tx hash.
	WireActionADD = byte(0)
	// WireActionDELETE signals that the entry's hash should be removed from
	// the cache. Content length is zero for DELETE entries.
	WireActionDELETE = byte(1)

	// WireV2Magic is the first byte of a v2 batch message. Not a unique
	// discriminator on its own — see the package comment above.
	WireV2Magic = byte(0xFF)
	// WireV2Version is the only v2 sub-version defined today. Bump if the
	// per-entry or header layout changes; receivers reject unknown sub-versions.
	WireV2Version = byte(0x02)
	// WireV2HeaderLen is the fixed-size prefix of a v2 message:
	// [magic][version][uint16 LE BucketsCount][uint32 LE entry count].
	WireV2HeaderLen = 8
	// WireV2BucketsOffset is where the producer's BucketsCount (uint16 LE)
	// lives in the v2 header - bytes 2 and 3, formerly reserved zeros.
	WireV2BucketsOffset = 2
	// WireV2MinEntrySize is the smallest possible v2 entry on the wire:
	// 8-byte xxhash + 32-byte hash + 1-byte action + 4-byte content length
	// (content length = 0 for a DELETE). Used by detectors to bound the
	// entry count against buffer size.
	WireV2MinEntrySize = 8 + 32 + 1 + 4

	// WireV1MinEntrySize is the smallest possible v1 entry on the wire:
	// 32-byte hash + 1-byte action + 4-byte content length (content length
	// = 0 for a DELETE). Used to clamp a wire-side entry count against the
	// remaining buffer so a malformed or malicious count cannot trigger an
	// oversized pre-loop allocation.
	WireV1MinEntrySize = 32 + 1 + 4
)

// EncodeDeleteBatch serializes a batch of DELETE entries in the v1 wire format:
// [uint32 LE entry count] then, per hash, [32 hash][WireActionDELETE][uint32 LE 0].
//
// v1 is chosen on purpose for producers that are not the validator: receivers
// detect v1 and v2 per message (see the package comment), and a v1 message needs
// neither the validator's xxhash bucket routing nor a producer built with manual
// partitioning, so any producer on the txmeta topic can use it. A count of 767
// (0x02FF) encodes as FF 02 00 00, the full v2 header signature, and a receiver
// may then parse the message as v2; callers must not send a batch of that size.
func EncodeDeleteBatch(hashes []chainhash.Hash) []byte {
	buf := make([]byte, 4+len(hashes)*WireV1MinEntrySize)
	binary.LittleEndian.PutUint32(buf, uint32(len(hashes))) //nolint:gosec // callers bound batch size well below MaxUint32

	off := 4

	for i := range hashes {
		copy(buf[off:], hashes[i][:])
		off += 32
		buf[off] = WireActionDELETE
		off++
		// content length stays zero for a DELETE
		off += 4
	}

	return buf
}

// MaxDeleteBatch caps the DELETE entries per message. 10,000 entries are 370 KB
// on the v1 wire and 450 KB on v2, well under Kafka's default 1 MiB message
// limit however large the demotion cascade.
const MaxDeleteBatch = 10_000

// V2LookalikeCount is the one v1 entry count whose little-endian bytes
// (FF 02 00 00) are the v2 header signature. A receiver can read a v1 message of
// this many entries as v2 and drop all of them, so no v1 producer sends one.
const V2LookalikeCount = int(WireV2Magic) | int(WireV2Version)<<8

// SplitDeleteBatches cuts hashes into consecutive batches of at most
// MaxDeleteBatch entries, none of them V2LookalikeCount long, so each batch can
// be sent as one message in either wire format. The batches share hashes'
// backing array.
func SplitDeleteBatches(hashes []chainhash.Hash) [][]chainhash.Hash {
	batches := make([][]chainhash.Hash, 0, len(hashes)/MaxDeleteBatch+1)

	for start := 0; start < len(hashes); {
		n := min(MaxDeleteBatch, len(hashes)-start)
		if n == V2LookalikeCount {
			n--
		}

		batches = append(batches, hashes[start:start+n])
		start += n
	}

	return batches
}
