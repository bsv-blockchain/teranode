package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
)

// QueryStaleCreatingTxs returns the hashes of transactions still carrying the
// creating bin whose unminedSince is in [1, unminedSinceBefore). It rides the
// existing unminedSince secondary index; the creating condition is applied
// server-side via a filter expression. Used by the pruner sweeper to roll
// forward abandoned tentative creates.
//
// The result is bounded: when limit > 0 the scan stops after that many hashes and
// the deferred recordset.Close() aborts the remaining partitions, so neither the
// server scan nor the returned slice grows unbounded with the unmined backlog. The
// caller treats a full page as "more may remain" and sweeps the rest next pass. The
// context is honoured so a shutdown mid-scan aborts promptly rather than draining.
func (s *Store) QueryStaleCreatingTxs(ctx context.Context, unminedSinceBefore uint32, limit int) ([]chainhash.Hash, error) {
	// unminedSinceBefore <= 1 leaves an empty [1, x) range; nothing to sweep.
	if unminedSinceBefore <= 1 {
		return nil, nil
	}

	stmt := aerospike.NewStatement(s.namespace, s.setName)
	stmt.BinNames = []string{fields.TxID.String()}

	if err := stmt.SetFilter(aerospike.NewRangeFilter(fields.UnminedSince.String(), 1, int64(unminedSinceBefore-1))); err != nil {
		return nil, errors.NewProcessingError("[QueryStaleCreatingTxs] failed to set unminedSince filter", err)
	}

	policy := util.GetAerospikeQueryPolicy(s.settings)
	policy.FilterExpression = aerospike.ExpBinExists(fields.Creating.String())

	recordset, err := s.client.QueryPartitions(policy, stmt, aerospike.NewPartitionFilterAll())
	if err != nil {
		return nil, errors.NewStorageError("[QueryStaleCreatingTxs] query failed", err)
	}

	defer recordset.Close()

	var hashes []chainhash.Hash

	for res := range recordset.Results() {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if res.Err != nil {
			return nil, errors.NewStorageError("[QueryStaleCreatingTxs] error iterating results", res.Err)
		}

		txIDBytes, ok := res.Record.Bins[fields.TxID.String()].([]byte)
		if !ok {
			// Only master records carry unminedSince (create.go writes it to batches[0]
			// only), so the index range filter never yields a split/child record. Guard
			// anyway: a record without a readable txID cannot be swept.
			continue
		}

		hash, err := chainhash.NewHash(txIDBytes)
		if err != nil {
			continue
		}

		hashes = append(hashes, *hash)

		if limit > 0 && len(hashes) == limit {
			// Deferred recordset.Close() aborts the remaining partitions.
			break
		}
	}

	return hashes, nil
}
