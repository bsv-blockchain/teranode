// Copyright (c) 2013-2018 The btcsuite developers
// Copyright (c) 2015-2018 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"github.com/libsv/go-bt/v2/chainhash"
)

// BlockLocator is used to help locate a specific block.  The algorithm for
// building the block locator is to add the hashes in reverse order until
// the genesis block is reached.  In order to keep the list of locator hashes
// to a reasonable number of entries, first the most recent previous 12 block
// hashes are added, then the step is doubled each loop iteration to
// exponentially decrease the number of hashes as a function of the distance
// from the block being located.
//
// For example, assume a block chain with a side chain as depicted below:
//
//	genesis -> 1 -> 2 -> ... -> 15 -> 16  -> 17  -> 18
//	                              \-> 16a -> 17a
//
// The block locator for block 17a would be the hashes of blocks:
// [17a 16a 15 14 13 12 11 10 9 8 7 6 4 genesis]
type BlockLocator []*chainhash.Hash

type BlockChain struct {
}

// BehaviorFlags is a bitmask defining tweaks to the normal behavior when
// performing chain processing and consensus rules checks.
type BehaviorFlags uint32

const (
	// BFFastAdd may be set to indicate that several checks can be avoided
	// for the block since it is already known to fit into the chain due to
	// already proving it correct links into the chain up to a known
	// checkpoint.  This is primarily used for headers-first mode.
	BFFastAdd BehaviorFlags = 1 << iota

	// BFNoPoWCheck may be set to indicate the proof of work check which
	// ensures a block hashes to a value less than the required target will
	// not be performed.
	BFNoPoWCheck

	// BFNoDupBlockCheck signals if the block should skip existence
	// checks.
	BFNoDupBlockCheck

	// BFNone is a convenience value to specifically indicate no flags.
	BFNone BehaviorFlags = 0
)
