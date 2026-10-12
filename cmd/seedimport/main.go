// Package main provides a CLI tool that loads a verifiable UTXO seed into a
// node's UTXO store. It reads a signed checkpoint and seed package from a blob
// store, verifies the checkpoint against compiled-in (or flag-provided) trusted
// authority keys, confirms the seed block is on the node's most-work header
// chain at the expected height, then streams the UTXO set into the UTXO store.
//
// The node process should be stopped before running this tool.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/cmd/seedimport/seedimport"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// defaultAuthorityPubKeys holds the compiled-in trusted authority public keys
// (compressed, hex-encoded). The release process fills this in with the
// official signing key(s); until then a key must be supplied via
// --authority-pubkey.
var defaultAuthorityPubKeys = []string{}

func main() {
	var (
		blockHashStr string
		seedURLStr   string
		authKeyHex   string
	)

	flag.StringVar(&blockHashStr, "block-hash", "", "Block hash of the seed to import (hex)")
	flag.StringVar(&seedURLStr, "seed-url", "", "Blob store URL holding the seed package and signed checkpoint")
	flag.StringVar(&authKeyHex, "authority-pubkey", "", "Trusted authority pubkey (compressed, hex) in addition to any compiled-in keys")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s --block-hash <hex> --seed-url <url> [--authority-pubkey <hex>]\n\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Loads a verifiable UTXO seed into the node's UTXO store.")
		fmt.Fprintln(flag.CommandLine.Output())
		flag.PrintDefaults()
	}

	flag.Parse()

	logger := ulogger.New("seedimport")
	s := settings.NewSettings()

	ctx := context.Background()

	if err := run(ctx, logger, s, blockHashStr, seedURLStr, authKeyHex); err != nil {
		logger.Errorf("seedimport failed: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger ulogger.Logger, s *settings.Settings, blockHashStr, seedURLStr, authKeyHex string) error {
	if blockHashStr == "" {
		return errors.NewConfigurationError("--block-hash is required")
	}

	if seedURLStr == "" {
		return errors.NewConfigurationError("--seed-url is required")
	}

	blockHash, err := chainhash.NewHashFromStr(blockHashStr)
	if err != nil {
		return errors.NewConfigurationError("invalid --block-hash %q", blockHashStr, err)
	}

	trustedKeys, err := seedimport.LoadTrustedKeys(defaultAuthorityPubKeys, authKeyHex)
	if err != nil {
		return err
	}

	seedURL, err := url.Parse(seedURLStr)
	if err != nil {
		return errors.NewConfigurationError("invalid --seed-url %q", seedURLStr, err)
	}

	hashPrefix := -2
	if seedURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(seedURL.Query().Get("hashPrefix"))
		if err != nil {
			return errors.NewConfigurationError("invalid hashPrefix in --seed-url", err)
		}
	}

	seedStore, err := blob.NewStore(logger, seedURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return errors.NewStorageError("failed to create seed store", err)
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, s, "seedimport", false)
	if err != nil {
		return errors.NewStorageError("failed to create utxo store", err)
	}

	bcStoreURL := s.BlockChain.StoreURL
	if bcStoreURL == nil {
		return errors.NewConfigurationError("blockchain store URL not found in config")
	}

	bcStore, err := blockchain.NewStore(logger, bcStoreURL, s)
	if err != nil {
		return errors.NewStorageError("failed to create blockchain store", err)
	}

	if s.ChainCfgParams == nil {
		return errors.NewConfigurationError("network parameters (ChainCfgParams) not found in config")
	}

	cfg := seedimport.Config{
		SeedStore:    seedStore,
		UTXOStore:    utxoStore,
		Lookup:       seedimport.NewBlockchainLookup(bcStore),
		TrustedKeys:  trustedKeys,
		BlockHash:    *blockHash,
		NetworkMagic: uint32(s.ChainCfgParams.Net),
	}

	return seedimport.Run(ctx, logger, cfg)
}
