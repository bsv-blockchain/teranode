// Package bitcoin provides utilities and structures for working with Bitcoin blockchain data.
//
// Usage:
//
// This package is designed to facilitate operations on Bitcoin data, including decoding,
// serialization, and database interactions.
//
// Functions:
//   - DecodeVarIntForIndex: Decodes variable-length integers from byte slices.
//   - DeserializeFileIndex: Converts serialized data into a FileIndex structure.
//   - IntToLittleEndianBytes: Converts integers to little-endian byte slices.
//
// Side effects:
//
// Functions in this package may interact with LevelDB databases, perform binary encoding/decoding,
// and handle errors related to Bitcoin data processing.
package bitcoin
