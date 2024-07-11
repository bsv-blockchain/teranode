package main

import (
	"context"
	"github.com/bitcoin-sv/ubsv/services/validator"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	txStore          blob.Store
	subtreeStore     blob.Store
	blockStore       blob.Store
	utxoStore        utxostore.Store
	blockchainStore  blockchainstore.Store
	blockchainClient blockchain.ClientI
	validatorClient  validator.Interface
)

func getUtxoStore(ctx context.Context, logger ulogger.Logger) utxostore.Store {
	if utxoStore != nil {
		return utxoStore
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}
	utxoStore, err = utxofactory.NewStore(ctx, logger, utxoStoreURL, "main")
	if err != nil {
		panic(err)
	}

	return utxoStore
}

func getBlockchainStore(ctx context.Context, logger ulogger.Logger) blockchainstore.Store {
	if blockchainStore != nil {
		return blockchainStore
	}

	blockchainURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no blockchain setting found")
	}
	blockchainStore, err = blockchainstore.NewStore(logger, blockchainURL)
	if err != nil {
		panic(err)
	}

	return blockchainStore
}

func getBlockchainClient(ctx context.Context, logger ulogger.Logger) blockchain.ClientI {
	if blockchainClient != nil {
		return blockchainClient
	}

	var err error
	blockchainClient, err = blockchain.NewClient(ctx, logger)
	if err != nil {
		panic(err)
	}

	return blockchainClient
}

func getValidatorClient(ctx context.Context, logger ulogger.Logger) validator.Interface {
	if validatorClient != nil {
		return validatorClient
	}

	var err error
	validatorClient, err = validator.NewClient(ctx, logger)
	if err != nil {
		panic(err)
	}

	return validatorClient
}

func getTxStore(logger ulogger.Logger) blob.Store {
	if txStore != nil {
		return txStore
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStore, err = blob.NewStore(logger, txStoreUrl)
	if err != nil {
		panic(err)
	}

	return txStore
}

func getSubtreeStore(logger ulogger.Logger) blob.Store {
	if subtreeStore != nil {
		return subtreeStore
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithPrefixDirectory(10))
	if err != nil {
		panic(err)
	}

	return subtreeStore
}

func getBlockStore(logger ulogger.Logger) blob.Store {
	if blockStore != nil {
		return blockStore
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}
	blockStore, err = blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	return blockStore
}
