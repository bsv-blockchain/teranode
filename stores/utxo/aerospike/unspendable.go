package aerospike

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

// batchUnspendable represents a batch operation to set the unspendable flag on a transaction
type batchUnspendable struct {
	ctx        context.Context
	txHash     chainhash.Hash
	childIndex uint32 // This will default to 0 which is the master record
	setValue   bool
	errCh      chan error // Channel for completion notification
}

func (s *Store) SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	g, ctx := errgroup.WithContext(ctx)

	for _, txHash := range txHashes {
		txHash := txHash

		g.Go(func() error {
			errCh := make(chan error, 1)

			s.unspendableBatcher.Put(&batchUnspendable{
				ctx:      ctx,
				txHash:   txHash,
				setValue: setValue,
				errCh:    errCh,
			})

			// Now we need to get totalRecords and do all the child records if necessary...

			return <-errCh
		})
	}

	return g.Wait()
}

// SetUnsUnspendable sets the unspendable flag on the given transactions in a batch
func (s *Store) setUnspendableBatch(batch []*batchUnspendable) {
	var (
		batchUDFPolicy = aerospike.NewBatchUDFPolicy()
		batchRecords   = make([]aerospike.BatchRecordIfc, 0, len(batch))
	)

	// Go through each batch item and set the tx to be unspendable
	for _, batchItem := range batch {
		// We will do the master record first...
		keySource := uaerospike.CalculateKeySource(&batchItem.txHash, batchItem.childIndex)

		key, err := aerospike.NewKey(s.namespace, s.setName, keySource)
		if err != nil {
			fmt.Printf("Failed to create key: %s\n", err)
			os.Exit(1)
		}

		// Now we need to get totalRecords and do all the child records if necessary...

		batchRecords = append(batchRecords, aerospike.NewBatchUDF(
			batchUDFPolicy,
			key,
			LuaPackage,
			"setUnspendable",
			aerospike.NewValue(batchItem.setValue),
		))
	}

	if err := s.client.BatchOperate(util.GetAerospikeBatchPolicy(s.settings), batchRecords); err != nil {
		for _, batchItem := range batch {
			batchItem.errCh <- errors.NewProcessingError("could not batch write unspendable flag", err)
		}

		return
	}

	// Now we need to get totalRecords and do all the child records if necessary...
	for idx, batchRecord := range batchRecords {
		if batchRecord.BatchRec().Err != nil {
			batch[idx].errCh <- errors.NewProcessingError("could not batch write unspendable flag", batchRecord.BatchRec().Err)
			continue
		}

		response := batchRecord.BatchRec().Record
		if response != nil && response.Bins != nil && response.Bins["SUCCESS"] != nil {
			responseMsg, ok := response.Bins["SUCCESS"].(string)
			if ok {
				parts := strings.Split(responseMsg, ":")
				if len(parts) != 2 {
					batch[idx].errCh <- errors.NewProcessingError("could not parse response", responseMsg)
					continue
				}

				if parts[0] != "OK" {
					batch[idx].errCh <- errors.NewProcessingError("could not parse response", responseMsg)
					continue
				}

				extraRecords, err := strconv.Atoi(parts[1])
				if err != nil {
					batch[idx].errCh <- errors.NewProcessingError("could not parse response", responseMsg)
					continue
				}

				if extraRecords == 0 {
					batch[idx].errCh <- nil
					continue
				}

				// We need to do the child records...
				g, _ := errgroup.WithContext(batch[idx].ctx)

				for i := 1; i <= extraRecords; i++ {
					i := i

					g.Go(func() error {
						errCh := make(chan error, 1)

						s.unspendableBatcher.Put(&batchUnspendable{
							txHash:     batch[idx].txHash,
							childIndex: uint32(i), // nolint:gosec
							setValue:   batch[idx].setValue,
							errCh:      errCh,
						})

						return <-errCh
					})
				}

				batch[idx].errCh <- g.Wait()
			}
		}
	}
}
