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
// When limit > 0 the returned slice holds at most that many hashes: the loop stops
// once it has them and the deferred recordset.Close() aborts the partitions not yet
// read. The limit does not bound the server scan. With fewer matches than the limit,
// the usual case when no record is creating, the server reads every record in the
// unminedSince range to apply the filter. The caller treats a full page as "more may
// remain" and takes the rest on later passes. The query keeps no cursor, and the page
// of a backlog above the limit is not the same from call to call: the server streams
// the partitions as they answer, so which records come first changes. The context is
// checked only when a result arrives, so a cancelled scan that finds no match runs on
// until the server finishes it.
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
