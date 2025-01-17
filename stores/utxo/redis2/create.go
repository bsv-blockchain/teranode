package redis2

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	redis_db "github.com/redis/go-redis/v9"
)

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	createOptions := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(createOptions)
	}

	_, _, deferFn := tracing.StartTracing(ctx, "redis:Create")
	defer deferFn()

	if len(tx.Outputs) == 0 {
		return nil, errors.NewProcessingError("tx %s has no outputs", tx.TxIDChainHash())
	}

	var txHash *chainhash.Hash

	if createOptions.TxID != nil {
		txHash = createOptions.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()

	if createOptions.IsCoinbase != nil {
		isCoinbase = *createOptions.IsCoinbase
	}

	var (
		fee          uint64
		utxoHashes   []*chainhash.Hash
		size         int
		extendedSize int
		err          error
	)

	if len(tx.Inputs) == 0 {
		fee = 0
		utxoHashes, err = utxo.GetUtxoHashes(tx, txHash)
	} else {
		size = tx.Size()
		extendedSize = len(tx.ExtendedBytes())
		fee, utxoHashes, err = utxo.GetFeesAndUtxoHashes(context.Background(), tx, blockHeight)
	}

	if err != nil {
		return nil, errors.NewProcessingError("failed to get fees and utxo hashes for %s", txHash, err)
	}

	fields := map[string]interface{}{
		"fee":          fee,
		"sizeInBytes":  size,
		"extendedSize": extendedSize,
		"spentUtxos":   0,
		"isCoinbase":   isCoinbase,
	}

	if createOptions.Conflicting {
		fields["conflicting"] = true
	}

	if len(createOptions.BlockIDs) > 0 {
		// Convert the blockIDs to a comma separated string
		var blockIDs strings.Builder

		for i, blockID := range createOptions.BlockIDs {
			if i > 0 {
				blockIDs.WriteString(",")
			}

			blockIDs.WriteString(strconv.FormatUint(uint64(blockID), 10))
		}

		fields["blockIDs"] = blockIDs.String()
	}

	if isCoinbase {
		// TODO - verify this is correct.  You cannot spend outputs that were created in a coinbase transaction
		// until 100 blocks have been mined on top of the block containing the coinbase transaction.
		// Bitcoin has a 100 block coinbase maturity period and the block in which the coinbase transaction is included is block 0.
		// counts as the 1st confirmation, so we need to wait for 99 more blocks to be mined before the coinbase outputs can be spent.
		// So, for instance an output from the coinbase transaction in block 9 can be spent in block 109.
		fields["spendingHeight"] = blockHeight + uint32(s.settings.ChainCfgParams.CoinbaseMaturity) //nolint:gosec
	}

	utxoCount := 0

	for i, output := range tx.Outputs {
		if output != nil {
			// store all coinbases, non-zero utxos and exceptions from pre-genesis
			if utxo.ShouldStoreOutputAsUTXO(isCoinbase, output, blockHeight) {
				fields[fmt.Sprintf("output:%d", i)] = hex.EncodeToString(output.Bytes())
				fields[fmt.Sprintf("utxo:%d", i)] = utxoHashes[i].String()
				utxoCount++
			}
		}
	}

	fields["nrUtxos"] = utxoCount

	// Store main transaction data only if key doesn't exist
	if err := s.client.Watch(ctx, func(tx *redis_db.Tx) error {
		// Check if key exists
		exists, err := tx.Exists(ctx, txHash.String()).Result()
		if err != nil {
			return err
		}

		if exists > 0 {
			return errors.NewTxExistsError("%v already exists in store", txHash)
		}

		// Start MULTI transaction
		_, err = tx.TxPipelined(ctx, func(pipe redis_db.Pipeliner) error {
			pipe.HSet(ctx, txHash.String(), fields)

			return nil
		})
		return err
	}, txHash.String()); err != nil {
		return nil, errors.NewStorageError("failed to store transaction", err)
	}

	if len(tx.Inputs) > 0 {
		if err := s.storeRawTransaction(ctx, txHash, tx, len(utxoHashes) > 0); err != nil {
			return nil, errors.NewStorageError("failed to store transaction in transaction store", err)
		}
	}

	return util.TxMetaDataFromTx(tx)
}

func (s *Store) storeRawTransaction(ctx context.Context, txHash *chainhash.Hash, tx *bt.Tx, hasUtxos bool) error {
	opts := []options.FileOption{
		options.WithFileExtension("tx"),
	}

	if !hasUtxos {
		// add a TTL to the transaction file, since there were no spendable utxos in the transaction
		opts = append(opts, options.WithTTL(s.expiration))
	}

	if err := s.transactionStore.Set(
		ctx,
		txHash[:],
		tx.ExtendedBytes(),
		opts...,
	); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
		return errors.NewStorageError("error writing transaction to transaction store [%s]", txHash.String(), err)
	}

	return nil
}
