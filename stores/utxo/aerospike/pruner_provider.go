package aerospike

import (
	"sync"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	aeropruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
)

// Ensure Store implements the pruner.PrunerProvider interface
var _ pruner.PrunerServiceProvider = (*Store)(nil)

// singleton instance of the pruner service
var (
	prunerServiceInstance pruner.Service
	prunerServiceMutex    sync.Mutex
	prunerServiceError    error
)

// ResetPrunerServiceForTests resets the pruner service singleton.
// This should only be called in tests to ensure clean state between test runs.
func ResetPrunerServiceForTests() {
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	prunerServiceInstance = nil
	prunerServiceError = nil
}

// GetPrunerService returns a pruner service for the Aerospike store.
// This implements the pruner.PrunerProvider interface.
func (s *Store) GetPrunerService() (pruner.Service, error) {
	// Check if DAH cleaner is disabled in settings
	if s.settings.UtxoStore.DisableDAHCleaner {
		return nil, nil
	}

	// Use a mutex to ensure thread safety when creating the singleton
	prunerServiceMutex.Lock()
	defer prunerServiceMutex.Unlock()

	// If the service has already been created, return it
	if prunerServiceInstance != nil {
		return prunerServiceInstance, prunerServiceError
	}

	// Create options for the pruner service.
	//
	// BuildAddDeletedChildrenRecord routes the pruner's per-parent update through
	// the native-op wrapper (subOpAddDeletedChildren), with transparent UDF
	// fallback when the cluster does not support wire op 200. This eliminates the
	// steady stream of batch_sub_udf calls the pruner used to emit even on
	// native-op-enabled deployments.
	//
	// A missing parent is the routine case for this op — the parent is normally
	// one the pruner itself already deleted — so the record must never be created.
	// initNativeTeranodeOps pins that for every native op by setting the shared
	// nativeOpBatchWritePolicy to UPDATE_ONLY, matching the Lua addDeletedChildren
	// this replaces (it returns TX_NOT_FOUND without writing when
	// aerospike:exists(rec) is false). A resurrected parent would hold only
	// deletedChildren — no txid, no deleteAtHeight — which the DAH scan could
	// never reclaim and extractTxHash would fail on. The miss therefore reaches
	// the pruner as KEY_NOT_FOUND, which it counts as a skipped parent.
	opts := aeropruner.Options{
		Logger:        s.logger,
		Ctx:           s.ctx,
		Client:        s.client,
		ExternalStore: s.externalStore,
		Namespace:     s.namespace,
		Set:           s.setName,
		IndexWaiter:   s,
		LuaPackage:    LuaPackage,
		BuildAddDeletedChildrenRecord: func(policy *aerospike.BatchUDFPolicy, key *aerospike.Key, childHashes []interface{}) aerospike.BatchRecordIfc {
			return s.teranodeBatchRecord(
				policy,
				LuaPackage,
				key,
				subOpAddDeletedChildren,
				"addDeletedChildren",
				childHashes,
			)
		},

		// Give the pruner the same runtime demotion every other native call site
		// has. The startup probe samples one partition master, so a mixed-version
		// cluster can pass it and then reject native writes with PARAMETER_ERROR
		// on other partitions. The pruner runs every block, making it a likely
		// first victim; without this it is the one native call site that cannot
		// trigger the fallback, and its parent updates would keep failing for the
		// process lifetime. demoteNativeOnUnsupported ignores everything that is
		// not a PARAMETER_ERROR, so handing it every error is safe.
		ObserveNativeOpError: s.demoteNativeOnUnsupported,
	}

	// Create a new pruner service
	prunerService, err := aeropruner.NewService(s.settings, opts)
	if err != nil {
		prunerServiceError = err
		return nil, err
	}

	// Store the singleton instance
	prunerServiceInstance = prunerService
	prunerServiceError = nil

	return prunerServiceInstance, nil
}
