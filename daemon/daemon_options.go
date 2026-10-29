package daemon

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Option is a functional option type for configuring the Daemon.
type Option func(*Daemon)

// WithLoggerFactory provides a custom logger factory for the Daemon and its services.
func WithLoggerFactory(factory func(serviceName string) ulogger.Logger) Option {
	return func(d *Daemon) {
		d.loggerFactory = factory
	}
}

// WithContext allows setting a custom context for the Daemon.
func WithContext(ctx context.Context) Option {
	return func(d *Daemon) {
		d.Ctx = ctx
	}
}

// StoreWrappers wraps the stores the daemon builds before any service receives
// them. Tests use it to put fault-injecting or recording decorators in front of
// the real stores. A nil field leaves that store unwrapped. A wrapper that embeds
// the store interface hides any method the concrete store has beyond it.
type StoreWrappers struct {
	UtxoStore       func(utxostore.Store) utxostore.Store
	BlockchainStore func(blockchainstore.Store) blockchainstore.Store
	SubtreeStore    func(blob.Store) blob.Store
	TxStore         func(blob.Store) blob.Store
	BlockStore      func(blob.Store) blob.Store
}

// WithStoreWrappers wraps the stores the Daemon builds with the given functions.
func WithStoreWrappers(wrappers StoreWrappers) Option {
	return func(d *Daemon) {
		d.daemonStores.wrappers = wrappers
	}
}
