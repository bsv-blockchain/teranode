// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package netsync

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg/chainhash"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// PeerNotifier exposes methods to notify peers of status changes to
// transactions, blocks, etc. Currently server (in the main package) implements
// this interface.
type PeerNotifier interface {
	// AnnounceNewTransactions(newTxs []*mempool.TxDesc)

	UpdatePeerHeights(latestBlkHash *chainhash.Hash, latestHeight int32, updateSource *peer.Peer)

	RelayInventory(invVect *wire.InvVect, data interface{})

	TransactionConfirmed(tx *bsvutil.Tx)
}

// Config is a configuration struct used to initialize a new SyncManager.
type Config struct {
	PeerNotifier PeerNotifier
	Chain        *blockchain.BlockChain
	ChainParams  *chaincfg.Params

	DisableCheckpoints bool
	MaxPeers           int

	MinSyncPeerNetworkSpeed uint64
}
