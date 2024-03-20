// Copyright (c) 2018 The bitcoinsv developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package merkleblock_test

import (
	"github.com/libsv/go-bt/v2/chainhash"
)

// hashFromStr provides function to wrap the primary function without
// the need to return an error since we can assert the static strings supplied
// in these tests will always decode
func hashFromStr(s string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(s)

	if err != nil {
		hash = &chainhash.Hash{}
	}

	return hash
}
