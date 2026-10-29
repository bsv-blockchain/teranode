package daemon

import (
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/bsv-blockchain/teranode/util/usql"
)

// The database handles under the stores. A crash closes these directly, so the
// stores' own Close, which drains their batchers first, never runs.
type (
	utxoRawDB         interface{ RawDB() *usql.DB }
	blockchainRawDB   interface{ GetDB() *usql.DB }
	aerospikeClientDB interface{ GetClient() *uaerospike.Client }
)

// crash stops the Daemon the way a killed process stops, as far as that can be
// done in-process. It closes the UTXO and blockchain database connections without
// draining the batchers in front of them, so queued writes fail instead of
// committing. It closes the libp2p host so peers see the node drop. It then
// cancels the services' context without calling any service's Stop(), and the
// daemon's own client and store shutdown is skipped.
//
// Goroutines that do not watch the context keep running until they next touch a
// closed connection; they cannot write to the closed databases. Blob stores and
// Kafka are left as they are: file blobs written before the crash stay on disk,
// and Kafka is external to the node.
func (d *Daemon) crash(logger ulogger.Logger) {
	d.crashed.Store(true)

	globalStoreMutex.RLock()
	utxoStore := d.daemonStores.rawUtxoStore
	blockchainStore := d.daemonStores.rawBlockchainStore
	globalStoreMutex.RUnlock()

	switch s := utxoStore.(type) {
	case utxoRawDB:
		if db := s.RawDB(); db != nil {
			if err := db.Close(); err != nil {
				logger.Warnf("[Daemon] crash: closing utxo database: %v", err)
			}
		}
	case aerospikeClientDB:
		if client := s.GetClient(); client != nil {
			client.Close()
		}
	}

	if s, ok := blockchainStore.(blockchainRawDB); ok {
		if db := s.GetDB(); db != nil {
			if err := db.Close(); err != nil {
				logger.Warnf("[Daemon] crash: closing blockchain database: %v", err)
			}
		}
	}

	if service, err := d.ServiceManager.GetService(serviceNameP2PFormal); err == nil {
		if server, ok := service.(*p2p.Server); ok && server.P2PClient != nil {
			if err := server.P2PClient.Close(); err != nil {
				logger.Warnf("[Daemon] crash: closing p2p node: %v", err)
			}
		}
	}

	d.ServiceManager.Kill()
	d.closeDoneOnce.Do(func() { close(d.doneCh) })
}
