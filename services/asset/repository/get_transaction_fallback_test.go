package repository_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// failingGetUtxoStore answers Get with a fixed error and delegates everything else
// to a real store.
type failingGetUtxoStore struct {
	utxo.Store
	err error
}

func (f failingGetUtxoStore) Get(context.Context, *chainhash.Hash, ...fields.FieldName) (*meta.Data, error) {
	return nil, f.err
}

func repositoryWithUtxoGetError(t *testing.T, utxoErr error) *repository.Repository {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)

	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///get_transaction_fallback")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	repo, err := repository.NewRepository(logger, settings, failingGetUtxoStore{Store: utxoStore, err: utxoErr},
		txStore, blockchainClient, nil, txStore, txStore, nil, nil)
	require.NoError(t, err)

	return repo
}

// TestRepository_GetTransaction_AFailingUtxoStoreIsNotAMiss pins that a UTXO store
// failure survives the fallback to the tx store. The fallback returned whatever
// the tx store said, so a UTXO store outage on a transaction held only there - any
// transaction seen inside a block rather than submitted here - was reported as
// "not found". The HTTP layer answers that 404, and getrawtransaction turns it
// into -5: the node denying a transaction it holds.
func TestRepository_GetTransaction_AFailingUtxoStoreIsNotAMiss(t *testing.T) {
	hash := chainhash.HashH([]byte("held only in the utxo store"))

	t.Run("an outage is reported as one", func(t *testing.T) {
		repo := repositoryWithUtxoGetError(t, errors.NewStorageError("aerospike get failed: i/o timeout"))

		_, err := repo.GetTransaction(context.Background(), &hash)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrStorageError, "the UTXO store's failure is the answer")
		require.NotErrorIs(t, err, errors.ErrNotFound)
		require.NotErrorIs(t, err, errors.ErrBlobNotFound)
		// The HTTP layer tells 404 from 500 by this phrase as well as by class.
		require.False(t, strings.Contains(err.Error(), "not found"), err.Error())
	})

	t.Run("a miss in both is still a miss", func(t *testing.T) {
		repo := repositoryWithUtxoGetError(t, errors.NewTxNotFoundError("%s not found", hash.String()))

		_, err := repo.GetTransaction(context.Background(), &hash)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found"),
			"the HTTP layer must still answer 404: %v", err)
	})
}
