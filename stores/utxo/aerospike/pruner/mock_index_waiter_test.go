package pruner

import (
	"context"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// MockIndexWaiter is a mock implementation of the IndexWaiter interface for testing
// It actually creates the index in Aerospike to ensure tests work properly
type MockIndexWaiter struct {
	Client                *uaerospike.Client
	Namespace             string
	Set                   string
	WaitForIndexReadyFunc func(ctx context.Context, indexName string) error
}

// WaitForIndexReady implements the IndexWaiter interface
// This implementation actually creates the index in Aerospike if it doesn't exist
func (m *MockIndexWaiter) WaitForIndexReady(ctx context.Context, indexName string) error {
	// If a custom function is provided, use it
	if m.WaitForIndexReadyFunc != nil {
		return m.WaitForIndexReadyFunc(ctx, indexName)
	}

	// Otherwise, create the index if it doesn't exist
	if m.Client != nil {
		// Check if the index already exists
		exists, err := m.indexExists(indexName)
		if err != nil {
			return err
		}

		if !exists {
			// Create the index
			writePolicy := aerospike.NewWritePolicy(0, 0)

			_, err := m.Client.CreateIndex(writePolicy, m.Namespace, m.Set, indexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC)
			if err != nil {
				if !strings.Contains(err.Error(), "Index already exists") {
					return err
				}
			}

			// Wait for the index to be built
			start := time.Now()

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				default:
					// Query index status
					node, err := m.Client.Client.Cluster().GetRandomNode()
					if err != nil {
						return err
					}

					policy := aerospike.NewInfoPolicy()

					infoMap, err := node.RequestInfo(policy, "sindex")
					if err != nil {
						return err
					}

					for _, v := range infoMap {
						if strings.Contains(v, "ns="+m.Namespace+":indexname="+indexName+":set="+m.Set) && strings.Contains(v, "RW") {
							return nil // Index is ready
						}
					}

					time.Sleep(100 * time.Millisecond)

					// Safety timeout after 30 seconds
					if time.Since(start) > 30*time.Second {
						return context.DeadlineExceeded
					}
				}
			}
		}
	}

	return nil
}

// indexExists checks if an index with the given name exists in the namespace
func (m *MockIndexWaiter) indexExists(indexName string) (bool, error) {
	if m.Client == nil || m.Client.Client == nil {
		return false, nil
	}

	// Get a random node from the cluster
	node, err := m.Client.Client.Cluster().GetRandomNode()
	if err != nil {
		return false, err
	}

	// Create an info policy
	policy := aerospike.NewInfoPolicy()

	// Request index information from the node
	infoMap, err := node.RequestInfo(policy, "sindex")
	if err != nil {
		return false, err
	}

	// Parse the response to check for the index
	for _, v := range infoMap {
		if strings.Contains(v, "ns="+m.Namespace+":indexname="+indexName+":set="+m.Set) {
			return true, nil
		}
	}

	return false, nil
}
