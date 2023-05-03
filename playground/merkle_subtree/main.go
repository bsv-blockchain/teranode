package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func main() {
	var err error

	t := time.Now()

	subTree, err := loadIds()
	if err != nil {
		subTree = NewTree(20) // 1,048,576 nodes
		fmt.Printf("NewTree took: %v\n", time.Since(t))

		t = time.Now()
		for i := 0; i < subTree.Size(); i++ {
			txID := make([]byte, 32)
			_, _ = rand.Read(txID)

			hash, _ := chainhash.NewHash(txID)

			if err = subTree.AddNode(hash); err != nil {
				panic(err)
			}
		}
		fmt.Printf("AddNode took: %v\n", time.Since(t))
		fmt.Printf("subTree: %v\n", len(subTree.Nodes))
		if err = storeIds(subTree); err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("Loading subtree from file took: %v\n", time.Since(t))
	}
	fmt.Printf("subTree: %d (%d)\n", len(subTree.Nodes), subTree.Size())

	t = time.Now()
	rootHash := subTree.RootHash()
	fmt.Printf("RootHash took: %v\n", time.Since(t))

	fmt.Printf("subTree rootHash: %v\n", rootHash)

	hash, _ := chainhash.NewHashFromStr("ee4abbb4ce3ca5157eb564e7339d7a0aae615c0303d6943288cd9d814e5a4d86")

	t = time.Now()
	newRootHash := subTree.ReplaceRootNode(hash)
	fmt.Printf("ReplaceRootNode took: %v\n", time.Since(t))
	fmt.Printf("new subTree rootHash: %v\n", newRootHash)
}

func storeIds(subTree *SubTree) error {
	f, err := os.Create("ids.txt")
	if err != nil {
		return err
	}
	defer f.Close()

	for _, id := range subTree.Nodes {
		_, err = f.WriteString(id.String() + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func loadIds() (*SubTree, error) {
	f, err := os.Open("ids.txt")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	subTree := NewTree(20) // 1,048,576 nodes

	scanner := bufio.NewScanner(f)
	var hash *chainhash.Hash
	for scanner.Scan() {
		hash, err = chainhash.NewHashFromStr(scanner.Text())
		if err != nil {
			return nil, err
		}
		_ = subTree.AddNode(hash)
	}

	return subTree, nil
}
