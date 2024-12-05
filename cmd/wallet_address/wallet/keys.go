package wallet

import (
	"flag"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2/bscript"
)

func Start() {
	var (
		privateKey string
		isMainnet  bool
	)

	flag.StringVar(&privateKey, "key", "", "Private key in WIF format")
	flag.BoolVar(&isMainnet, "mainnet", true, "Is a mainnet address required?")
	flag.Parse()

	if privateKey == "" {
		fmt.Println("private key is required. Use -key flag")
		return
	}

	address, err := GenerateWalletAddress(privateKey, isMainnet)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Wallet Address: %s\n", address)
}

func GenerateWalletAddress(privateKey string, isMainnet bool) (address string, err error) {
	pk, err := wif.DecodeWIF(privateKey)
	if err != nil {
		return "", errors.NewProcessingError("can't decode priv key", err)
	}

	walletAddress, err := bscript.NewAddressFromPublicKey(pk.PrivKey.PubKey(), isMainnet)
	if err != nil {
		return "", errors.NewProcessingError("can't create address", err)
	}

	return walletAddress.AddressString, nil
}
