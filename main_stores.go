package main

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/txmeta"
	"github.com/bitcoin-sv/ubsv/services/txmeta/store"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmetastore "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxo_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

var (
	txStore      blob.Store
	subtreeStore blob.Store
	txMetaStore  txmetastore.Store
	utxoStore    utxostore.Interface
)

func getTxMetaStore(logger ulogger.Logger) txmetastore.Store {
	if txMetaStore != nil {
		return txMetaStore
	}
	// append the serviceName to the key so that you can a tweaked setting for each service if need be.
	// if not found it reverts to the non-appended key. nice
	// used for asset_service so that it has a connection to aerospike but doesn't set min connections to 512 (only needs a handful)
	serviceName, _ := gocore.Config().Get("SERVICE_NAME", "ubsv")
	key := fmt.Sprintf("txmeta_store_%s", serviceName)
	txMetaStoreURL, err, found := gocore.Config().GetURL(key)
	if err != nil {
		txMetaStoreURL, err, found = gocore.Config().GetURL("txmeta_store")
	}
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no txmeta_store setting found")
	}

	if txMetaStoreURL.Scheme == "memory" {
		// the memory store is reached through a grpc client
		txMetaStore, err = txmeta.NewClient(context.Background(), logger)
		if err != nil {
			panic(err)
		}
	} else {
		txMetaStore, err = store.New(logger, txMetaStoreURL)
		if err != nil {
			panic(err)
		}
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
	utxoStore, err = utxo_factory.NewStore(ctx, logger, utxoStoreURL, "main")
	if err != nil {
		panic(err)
	}

	return utxoStore
}

func getUtxoMemoryStore() utxostore.Interface {
	utxoStoreURL, err, _ := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if utxoStoreURL.Scheme != "memory" {
		panic("utxo grpc server only supports memory store")
	}

	var s utxostore.Interface
	switch utxoStoreURL.Path {
	case "/splitbyhash":
		s = memory.NewSplitByHash(true)
	default:
		s = memory.New(true)
	}
	return s
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
