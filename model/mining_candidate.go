package model

import (
	"crypto/rand"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/gocore"
)

// CreateCoinbaseTxCandidate creates a coinbase transaction for the mining candidate
// p2pk is an optional parameter to specify if the coinbase output should be a pay to public key
// instead of a pay to public key hash
func (mc *MiningCandidate) CreateCoinbaseTxCandidate(p2pk ...bool) (*bt.Tx, error) {
	// Create a new coinbase transaction
	arbitraryText, _ := gocore.Config().Get("coinbase_arbitrary_text", "/TERANODE/")

	coinbasePrivKeys, found := gocore.Config().GetMulti("miner_wallet_private_keys", "|")
	if !found {
		return nil, errors.NewConfigurationError("miner_wallet_private_keys not found in config")
	}

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := wif.DecodeWIF(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	a, b, err := GetCoinbaseParts(mc.Height, mc.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	if len(p2pk) > 0 && p2pk[0] {
		coinbaseTx.Version = 2

		// reset outputs
		coinbaseTx.Outputs = []*bt.Output{}

		// Add 1 coinbase output as a pay to public key
		privateKey, err := wif.DecodeWIF(coinbasePrivKeys[0])
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		lockingScript := &bscript.Script{} // NewFromBytes(append(privateKey.SerialisePubKey(), bscript.OpCHECKSIG))
		_ = lockingScript.AppendPushData(privateKey.SerialisePubKey())
		_ = lockingScript.AppendOpcodes(bscript.OpCHECKSIG)

		coinbaseTx.AddOutput(&bt.Output{
			Satoshis:      mc.CoinbaseValue,
			LockingScript: lockingScript,
		})
	}

	return coinbaseTx, nil
}

func (mc *MiningCandidate) CreateCoinbaseTxCandidateForAddress(address *string) (*bt.Tx, error) {
	arbitraryText, _ := gocore.Config().Get("coinbase_arbitrary_text", "/TERANODE/")

	if address == nil {
		return nil, errors.NewConfigurationError("address is required for ")
	}

	a, b, err := GetCoinbaseParts(mc.Height, mc.CoinbaseValue, arbitraryText, []string{*address})
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	return coinbaseTx, nil
}
