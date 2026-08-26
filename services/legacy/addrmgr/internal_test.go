// Copyright (c) 2013-2015 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package addrmgr

import (
	"time"

	"github.com/bsv-blockchain/go-wire"
)

func TstKnownAddressIsBad(ka *KnownAddress) bool {
	return ka.isBad()
}

func TstKnownAddressChance(ka *KnownAddress) float64 {
	return ka.chance()
}

func TstNewKnownAddress(na *wire.NetAddress, attempts int,
	lastattempt, lastsuccess time.Time, tried bool, refs int) *KnownAddress {
	return &KnownAddress{na: na, attempts: attempts, lastattempt: lastattempt,
		lastsuccess: lastsuccess, tried: tried, refs: refs}
}

// TstKnownAddressTried reports whether the address sits in the tried table.
// Exported for the tests only, so they can tell where a draw came from without
// adding a production accessor nothing in production would call.
func TstKnownAddressTried(ka *KnownAddress) bool {
	return ka.tried
}

// TstAddressIsTried is the same question asked of the manager rather than of an
// entry, for callers that only hold the address. UnverifiedAddress no longer
// hands out the KnownAddress, deliberately, so this takes a.mtx to look it up
// the way any other reader has to.
func TstAddressIsTried(a *AddrManager, na *wire.NetAddress) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	ka := a.find(na)

	return ka != nil && ka.tried
}
