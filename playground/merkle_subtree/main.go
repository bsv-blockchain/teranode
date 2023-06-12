package main

import (
	"bufio"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
)

func main() {
	treeSize := flag.Int("treeSize", 20, "tree size") // 1,048,576 nodes

	flag.Parse()

	var err error

	t := time.Now()

	subtree, err := loadIds(*treeSize)
	if err != nil {
		subtree = NewTree(*treeSize)
		fmt.Printf("NewTree took: %v\n", time.Since(t))

		t = time.Now()
		for i := 0; i < subtree.Size(); i++ {
			txID := make([]byte, 32)
			_, _ = rand.Read(txID)

			if err = subtree.AddNode([32]byte(txID), 111); err != nil {
				panic(err)
			}
		}
		fmt.Printf("AddNode took: %v\n", time.Since(t))
		fmt.Printf("subtree: %v\n", len(subtree.TxHashes))
		if err = storeIds(subtree, *treeSize); err != nil {
			panic(err)
		}
	} else {
		fmt.Printf("Loading subtree from file took: %v\n", time.Since(t))
	}
	fmt.Printf("subtree: %d (%d)\n", len(subtree.TxHashes), subtree.Size())

	t = time.Now()
	rootHash := subtree.RootHash()
	fmt.Printf("RootHash took: %v\n", time.Since(t))

	fmt.Printf("subtree rootHash: %v\n", utils.ReverseAndHexEncodeHash(rootHash))

	hash, _ := chainhash.NewHashFromStr("ee4abbb4ce3ca5157eb564e7339d7a0aae615c0303d6943288cd9d814e5a4d86")

	t = time.Now()
	newRootHash := subtree.ReplaceRootNode(*hash)
	fmt.Printf("ReplaceRootNode took: %v\n", time.Since(t))
	fmt.Printf("newSubtree rootHash: %v\n", utils.ReverseAndHexEncodeHash(newRootHash))
}

func storeIds(subtree *Subtree, treeSize int) error {
	f, err := os.Create(fmt.Sprintf("ids-%d.txt", treeSize))
	if err != nil {
		return err
	}
	defer f.Close()

	for _, id := range subtree.TxHashes {
		_, err = f.WriteString(utils.ReverseAndHexEncodeHash(id) + "\n")
		if err != nil {
			return err
		}
	}

	return nil
}

func loadIds(treeSize int) (*Subtree, error) {
	f, err := os.Open(fmt.Sprintf("ids-%d.txt", treeSize))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	subtree := NewTree(treeSize) // 1,048,576 nodes

	scanner := bufio.NewScanner(f)
	var hash *chainhash.Hash
	for scanner.Scan() {
		hash, err = chainhash.NewHashFromStr(scanner.Text())
		if err != nil {
			return nil, err
		}
		_ = subtree.AddNode(*hash, 111)
	}

	return subtree, nil
}
