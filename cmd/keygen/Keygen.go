package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
)

func main() {
	hex, err := generateHexEncodedEd25519PrivateKey()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(hex)

	pk, err1 := decodeHexEd25519PrivateKey(hex)
	if err1 != nil {
		fmt.Println(err1)
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/%s/tcp/%d", "0.0.0.0", 9095)),
		libp2p.Identity(pk),
	)
	if err != nil {
		panic(err)
	}

	fmt.Printf("[P2PNode] peer ID: %s\n", h.ID().Pretty())
	fmt.Printf("[P2PNode] Connect to me on:\n")
	for _, addr := range h.Addrs() {
		fmt.Printf("[P2PNode]   %s/p2p/%s\n", addr, h.ID().Pretty())
	}
}

func generateHexEncodedEd25519PrivateKey() (hexString string, err error) {
	privateKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return hexString, err
	}

	raw, err := privateKey.Raw()
	if err != nil {
		return hexString, err
	}

	// Encode the private key as a hex string.
	hexEncodedPrivateKey := hex.EncodeToString(raw)
	return hexEncodedPrivateKey, nil
}

func decodeHexEd25519PrivateKey(hexEncodedPrivateKey string) (crypto.PrivKey, error) {
	privKeyBytes, err := hex.DecodeString(hexEncodedPrivateKey)
	if err != nil {
		return nil, err
	}

	privKey, err := crypto.UnmarshalEd25519PrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}

	return privKey, nil
}
