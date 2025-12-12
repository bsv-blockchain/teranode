package main

import (
	"crypto/sha256"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"golang.org/x/crypto/ripemd160" //nolint:gosec // this is a known safe use of ripemd160 for Bitcoin address generation
)

func main() {
	// Verify 64-bit architecture
	if strconv.IntSize != 64 {
		log.Printf("Error: keypairgen requires a 64-bit architecture. Current architecture: %s\n", runtime.GOARCH)
		os.Exit(1)
	}

	// Generate a new private key
	privateKey, err := bec.NewPrivateKey()
	if err != nil {
		log.Fatal("Failed to generate private key:", err)
	}

	fmt.Printf("Private key:         %x\n", privateKey.Serialize())

	// Print public key
	fmt.Printf("Public key:          %x\n", privateKey.PubKey().Compressed())

	// Print public key hash (SHA256 + RIPEMD160)
	pubKeyBytes := privateKey.PubKey().Compressed()
	sha256Hash := sha256.Sum256(pubKeyBytes)
	ripemd160Hasher := ripemd160.New() //nolint:gosec // this is a known safe use of ripemd160 for Bitcoin address generation
	ripemd160Hasher.Write(sha256Hash[:])
	pubKeyHash := ripemd160Hasher.Sum(nil)
	fmt.Printf("Public key hash:     %x\n", pubKeyHash)

	// Get the WIF (Wallet Import Format) string for mainnet
	wif := privateKey.Wif()
	fmt.Println("WIF:                ", wif)

	// Generate address from public key
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	if err != nil {
		log.Fatal("Failed to create address:", err)
	}
	fmt.Println("Mainnet Address:    ", address.AddressString)

	// Generate address from public key
	addressTestnet, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), false)
	if err != nil {
		log.Fatal("Failed to create address:", err)
	}
	fmt.Println("Non-mainnet Address:", addressTestnet.AddressString)
}
