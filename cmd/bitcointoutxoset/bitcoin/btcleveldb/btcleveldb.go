// Package btcleveldb provides utilities for interacting with Bitcoin's LevelDB-based chainstate and block index databases.
//
// Usage:
//
//	This package is typically used to decode, read, and process Bitcoin chainstate and block index data stored in LevelDB format.
//	It is intended for use in tools that extract, analyze, or convert UTXO and block metadata from Bitcoin nodes.
//
// Functions:
//   - Functions for reading, decoding, and interpreting LevelDB keys and values, including varInt encoding/decoding and UTXO record parsing.
//   - Utilities for handling Bitcoin-specific data structures and compression formats found in LevelDB.
//
// Side effects:
//
//	Functions in this package may read from disk and process raw binary data, but do not perform network or database writes.
package btcleveldb
