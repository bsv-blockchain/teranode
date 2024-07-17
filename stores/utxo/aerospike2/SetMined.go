// //go:build aerospike

package aerospike2

import (
	"context"
	"math"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/aerospike/aerospike-client-go/v7/types"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) SetMinedMulti(_ context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	batchPolicy := util.GetAerospikeBatchPolicy()

	// math.MaxUint32 - 1 does not update expiration of the record
	policy := util.GetAerospikeBatchWritePolicy(0, math.MaxUint32-1)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	batchRecords := make([]aerospike.BatchRecordIfc, len(hashes))

	for idx, hash := range hashes {
		key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
		if err != nil {
			return errors.New(errors.ERR_PROCESSING, "aerospike NewKey error", err)
		}
		op := aerospike.ListAppendOp("blockIDs", blockID)
		record := aerospike.NewBatchWrite(policy, key, op)
		// Add to batch
		batchRecords[idx] = record
	}

	err := s.client.BatchOperate(batchPolicy, batchRecords)
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "aerospike BatchOperate error", err)
	}

	prometheusTxMetaAerospikeMapSetMinedBatch.Inc()

	okUpdates := 0
	var errs error
	nrErrors := 0
	for idx, batchRecord := range batchRecords {
		err = batchRecord.BatchRec().Err
		if err != nil {
			var aErr *aerospike.AerospikeError
			if errors.As(err, &aErr) && aErr != nil && aErr.ResultCode == types.KEY_NOT_FOUND_ERROR {
				// the tx Meta does not exist anymore, so we do not have to set the mined status
				continue
			}
			errs = errors.Join(errs, errors.New(errors.ERR_STORAGE_ERROR, "aerospike batchRecord error: %s", hashes[idx].String(), err))
			nrErrors++
		} else {
			okUpdates++
		}
	}

	prometheusTxMetaAerospikeMapSetMinedBatchN.Add(float64(okUpdates))

	if errs != nil || nrErrors > 0 {
		prometheusTxMetaAerospikeMapSetMinedBatchErrN.Add(float64(nrErrors))
		return errors.New(errors.ERR_ERROR, "aerospike batchRecord errors", errs)
	}

	return nil
}

func (s *Store) SetMined(_ context.Context, hash *chainhash.Hash, blockID uint32) error {
	policy := util.GetAerospikeWritePolicy(0, math.MaxUint32)
	policy.RecordExistsAction = aerospike.UPDATE_ONLY

	key, err := aerospike.NewKey(s.namespace, s.setName, hash[:])
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "aerospike NewKey error", err)
	}

	_, err = s.client.Operate(policy, key, aerospike.ListAppendOp("blockIDs", blockID))
	if err != nil {
		return errors.New(errors.ERR_STORAGE_ERROR, "aerospike Operate error", err)
	}

	prometheusTxMetaAerospikeMapSetMined.Inc()

	return nil
}
