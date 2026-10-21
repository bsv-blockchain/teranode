package txmetacache

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
