package main

import (
	"fmt"
	"log"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
)

func main() {
	// Generate a new private key
	privateKey, err := bec.NewPrivateKey()
	if err != nil {
		log.Fatal("Failed to generate private key:", err)
	}

	// Get the WIF (Wallet Import Format) string
	wif := privateKey.Wif()
	fmt.Println("WIF:", wif)

	// Generate address from public key
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		log.Fatal("Failed to create address:", err)
	}
	fmt.Println("Address:", address.AddressString)
}
