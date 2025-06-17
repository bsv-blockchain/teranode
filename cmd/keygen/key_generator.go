// Package keygen provides utilities for generating cryptographic keys.
// It is designed to facilitate the creation of secure private keys and their integration
// into libp2p hosts for peer-to-peer networking.
//
// Usage:
//
// This package is typically used as a command-line tool to generate Ed25519 private keys,
// encode them as hex strings, and decode them back for use in libp2p applications.
//
// Functions:
//   - generateHexEncodedEd25519PrivateKey: Generates a new Ed25519 private key and encodes it as a hex string.
//   - decodeHexEd25519PrivateKey: Decodes a hex-encoded Ed25519 private key back to its original form.
//
// Side effects:
//
// Functions in this package may print to stdout and exit the process if an error occurs.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

// main is the entry point of the keygen package. It generates a new Ed25519 private key,
// encodes it as a hex string, decodes it back, and uses it to create a libp2p host.
//
// This function demonstrates the key generation and decoding process, and prints the
// generated key and libp2p host information to stdout. It exits the process if any error occurs.
func main() {
	// Generate a new Ed25519 private key and encode it as a hex string.
	hexEncoded, err := generateHexEncodedEd25519PrivateKey()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(hexEncoded)

	var privateKey crypto.PrivKey

	// Decode the hex string back to an Ed25519 private key.
	privateKey, err = decodeHexEd25519PrivateKey(hexEncoded)
	if err != nil {
		log.Fatal(err)
	}

	var libHost host.Host

	// Create a new host with the decoded private key.
	libHost, err = libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", "0.0.0.0", 9095)),
		libp2p.Identity(privateKey),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("[P2PNode] Peer ID: %s\n", libHost.ID().String())
	fmt.Printf("[P2PNode] Connect to me on:\n")

	// Print the addresses
	for _, addr := range libHost.Addrs() {
		fmt.Printf("[P2PNode]   %s/p2p/%s\n", addr, libHost.ID().String())
	}
}

// generateHexEncodedEd25519PrivateKey generates a new Ed25519 private key and returns it as a hex-encoded string.
func generateHexEncodedEd25519PrivateKey() (hexString string, err error) {
	var privateKey crypto.PrivKey

	// Generate a new Ed25519 private key.
	privateKey, _, err = crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return
	}

	var raw []byte

	// Extract the raw bytes from the private key.
	raw, err = privateKey.Raw()
	if err != nil {
		return
	}

	// Encode the private key as a hex string.
	hexString = hex.EncodeToString(raw)

	return
}

// decodeHexEd25519PrivateKey decodes a hex-encoded Ed25519 private key and returns it as a crypto.PrivKey.
func decodeHexEd25519PrivateKey(hexEncodedPrivateKey string) (privKey crypto.PrivKey, err error) {
	var privKeyBytes []byte

	// Decode the hex string into raw bytes.
	privKeyBytes, err = hex.DecodeString(hexEncodedPrivateKey)
	if err != nil {
		return
	}

	// Unmarshal the raw bytes into an Ed25519 private key.
	privKey, err = crypto.UnmarshalEd25519PrivateKey(privKeyBytes)

	return
}
