// genpeerkeys regenerates compose/gen/peer_keys.json with N libp2p Ed25519
// keypairs. Indices 1..3 reuse the keys hardcoded in compose/settings_test.conf
// so that the generated N=3 setup is behaviourally identical to the existing
// docker-compose-3blasters.yml. Indices 4..N are freshly generated.
//
// Usage: go run ./compose/cmd/genpeerkeys -n 10 -o compose/gen/peer_keys.json
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

type keyEntry struct {
	Index      int    `json:"index"`
	PeerID     string `json:"peer_id"`
	PrivateKey string `json:"private_key_hex"`
}

// Existing keys from compose/settings_test.conf:171-184. Kept here so N=3
// renders match the historical compose stack byte-for-byte (peer wise).
var seedKeys = []keyEntry{
	{
		Index:      1,
		PeerID:     "12D3KooWAFXWuxgdJoRsaA4J4RRRr8yu6WCrAPf8FaS7UfZg3ceG",
		PrivateKey: "c8a1b91ae120878d91a04c904e0d565aa44b2575c1bb30a729bd3e36e2a1d5e6067216fa92b1a1a7e30d0aaabe288e25f1efc0830f309152638b61d84be6b71d",
	},
	{
		Index:      2,
		PeerID:     "12D3KooWG6aCkDmi5tqx4G4AvVDTQdSVvTSzzQvk1vh9CtSR8KEW",
		PrivateKey: "89a2d8acf5b2e60fd969914c326c63cde50675a47897c0eaacc02eb6ff8665585d4d059f977910472bcb75040617632019cc0749443fdc66d331b61c8cfb4b0f",
	},
	{
		Index:      3,
		PeerID:     "12D3KooWHHeTM3aK4s9DKS6DQ7SbBb7czNyJsPZtQiUKa4fduMB9",
		PrivateKey: "d77a7cac7833f2c0263ed7b9aaeb8dda1effaf8af948d570ed8f7a93bd3c418d6efee7bdd82ddb80484be84ba0c78ea07251a3ba2b45b2b3367fd5e2f0284e7c",
	},
}

func main() {
	n := flag.Int("n", 10, "number of keys to produce")
	out := flag.String("o", "compose/cmd/gennodes/peer_keys.json", "output path")
	flag.Parse()

	if *n < 1 || *n > 64 {
		fmt.Fprintln(os.Stderr, "n must be between 1 and 64")
		os.Exit(2)
	}

	keys := make([]keyEntry, 0, *n)
	for i := 1; i <= *n; i++ {
		if i <= len(seedKeys) {
			keys = append(keys, seedKeys[i-1])
			continue
		}
		k, err := freshKey(i)
		if err != nil {
			fmt.Fprintf(os.Stderr, "generate key %d: %v\n", i, err)
			os.Exit(1)
		}
		keys = append(keys, k)
	}

	data, err := json.MarshalIndent(keys, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", *out, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d keys to %s\n", *n, *out)
}

func freshKey(index int) (keyEntry, error) {
	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return keyEntry{}, err
	}
	raw, err := priv.Raw()
	if err != nil {
		return keyEntry{}, err
	}
	id, err := peer.IDFromPublicKey(pub)
	if err != nil {
		return keyEntry{}, err
	}
	return keyEntry{
		Index:      index,
		PeerID:     id.String(),
		PrivateKey: hex.EncodeToString(raw),
	}, nil
}
