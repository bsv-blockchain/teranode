// Copyright (c) 2013-2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bsvutil

const (
	// SatoshiPerBitcent is the number of satoshi in one bitcoin cent.
	SatoshiPerBitcent = 1_000_000

	// SatoshiPerBitcoin is the number of satoshi in one bitcoin (1 BTC).
	SatoshiPerBitcoin = 100_000_000

	// MaxSatoshi is the maximum transaction amount allowed in satoshi.
	MaxSatoshi = 21_000_000 * SatoshiPerBitcoin
)
