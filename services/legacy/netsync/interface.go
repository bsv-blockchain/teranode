// Copyright (c) 2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package netsync provides network synchronization functionality for the legacy Bitcoin protocol.
// It handles peer coordination, block synchronization, and transaction relay operations.
package netsync

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// PeerNotifier exposes methods to notify peers of status changes to
// transactions, blocks, etc. Currently, server (in the main package) implements
// this interface.
type PeerNotifier interface {
	// AnnounceNewTransactions notifies all connected peers about new transactions
	// that have been added to the memory pool and are ready for relay.
	AnnounceNewTransactions(newTxs []*TxHashAndFee)

	// UpdatePeerHeights updates the known block heights of connected peers based on
	// the latest block information, using the specified peer as the update source.
	UpdatePeerHeights(latestBlkHash *chainhash.Hash, latestHeight int32, updateSource *peer.Peer)

	// RelayInventory broadcasts inventory messages to connected peers to announce
	// the availability of new blocks, transactions, or other data.
	RelayInventory(invVect *wire.InvVect, data interface{})

	// TransactionConfirmed notifies peers that a transaction has been confirmed
	// by inclusion in a block, typically used for cleanup and state updates.
	TransactionConfirmed(tx *bsvutil.Tx)
}

// Config is a configuration struct used to initialize a new SyncManager.
// It contains all the necessary dependencies and settings required for
// peer synchronization and blockchain management operations.
type Config struct {
	// PeerNotifier provides the interface for notifying peers about network events
	// such as new transactions, blocks, and inventory updates.
	PeerNotifier PeerNotifier
	// ChainParams defines the blockchain network parameters (mainnet, testnet, regtest)
	// that determine consensus rules and network-specific constants.
	ChainParams *chaincfg.Params

	// DisableCheckpoints when true, disables the use of hardcoded checkpoints
	// for faster initial blockchain synchronization.
	DisableCheckpoints bool
	// MaxPeers specifies the maximum number of peers that can be connected
	// simultaneously for synchronization operations.
	MaxPeers int

	// MinSyncPeerNetworkSpeed defines the minimum network speed (in bytes per second)
	// required for a peer to be considered suitable for blockchain synchronization.
	MinSyncPeerNetworkSpeed uint64
}
