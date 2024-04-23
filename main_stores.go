package main

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	txmetafactory "github.com/bitcoin-sv/ubsv/stores/txmeta/_factory"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	txStore      blob.Store
	subtreeStore blob.Store
	blockStore   blob.Store
	txMetaStore  txmetastore.Store
	utxoStore    utxostore.Interface
)

func getTxMetaStore(logger ulogger.Logger) txmetastore.Store {
	if txMetaStore != nil {
		return txMetaStore
	}
	txMetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no txmeta_store setting found")
	}

	txMetaStore, err = txmetafactory.New(logger, txMetaStoreURL)
	if err != nil {
		panic(err)
	}

	return txMetaStore
}

func getUtxoStore(ctx context.Context, logger ulogger.Logger) utxostore.Interface {
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
	blockStore, err = blob.NewStore(logger, blockStoreUrl, options.WithPrefixDirectory(2))
	if err != nil {
		panic(err)
	}

	return blockStore
}
