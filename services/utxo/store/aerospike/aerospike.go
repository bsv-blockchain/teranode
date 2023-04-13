package aerospike

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	aero "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type Store struct {
	client    *aero.Client
	namespace string
}

func New(host string, port int, namespace string) (*Store, error) {
	client, err := aero.NewClient(host, port)
	if err != nil {
		return nil, err
	}

	return &Store{
		client:    client,
		namespace: namespace,
	}, nil
}

func (s Store) Get(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	return nil, nil
}

func (s Store) Store(_ context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	policy := aero.NewWritePolicy(0, 0)
	policy.RecordExistsAction = aero.CREATE_ONLY
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}

	bins := aero.BinMap{
		"txid": []byte{},
	}
	err = s.client.Put(policy, key, bins)
	if err != nil {
		// check whether we already set this utxo
		value, getErr := s.client.Get(nil, key, "txid")
		if value != nil && getErr == nil {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		}

		if getErr.Error() == types.ResultCodeToString(types.KEY_NOT_FOUND_ERROR) {
			return &store.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil
		}

		return nil, err
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK), // should be created, we need this for the block assembly
	}, nil
}

func (s Store) Spend(_ context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	policy := aero.NewWritePolicy(1, 0)
	policy.RecordExistsAction = aero.UPDATE_ONLY
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}
	bins := aero.BinMap{
		"txid": txID[:],
	}

	err = s.client.Put(policy, key, bins)
	if err != nil {
		// check whether we had the same value set as before
		value, getErr := s.client.Get(nil, key, "txid")
		if getErr != nil {
			return nil, getErr
		}
		valueBytes, ok := value.Bins["txid"].([]byte)
		if ok {
			if [32]byte(valueBytes) == [32]byte(txID[:]) {
				return &store.UTXOResponse{
					Status: int(utxostore_api.Status_OK),
				}, nil
			}
		}
		return nil, err
	}

	return &store.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s Store) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	policy := aero.NewWritePolicy(2, 0)
	policy.GenerationPolicy = aero.EXPECT_GEN_EQUAL
	policy.CommitLevel = aero.COMMIT_ALL // strong consistency

	key, err := aero.NewKey(s.namespace, "utxo", hash[:])
	if err != nil {
		return nil, err
	}

	_, err = s.client.Delete(policy, key)
	if err != nil {
		return nil, err
	}

	return s.Store(ctx, hash)
}
