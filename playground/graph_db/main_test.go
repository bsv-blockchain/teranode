package main

import (
	"fmt"
	"log"
	mathRand "math/rand"
	"testing"
	"time"
)

const (
	unchainedTransactionCount = 1_000_000
	maxChainLength            = 10
	chainCount                = 200
	subtreeCount              = 3
	blockCount                = 3

	neoTestUrl      = "neo4j://localhost:7687"
	neoTestUsername = "neo4j"
	neoTestPassword = "localneo"
)

func TestNeo4j(t *testing.T) {

	txArray = make([]string, 0)

	neoClient, err := NewNeoClient(neoTestUrl, neoTestUsername, neoTestPassword)
	if err != nil {
		log.Fatalf("Error creating NeoClient: %s", err.Error())
	}
	defer neoClient.Close()
	transactions := make([]*TxInfo, unchainedTransactionCount)

	start := time.Now()
	for i := range transactions {
		t := generateRandomTxInfo(1, 1)
		transactions[i] = t
		txArray = append(txArray, t.Hash.String())
	}
	fmt.Printf("Time to generate txids: %v\n", time.Since(start))
	start = time.Now()
	err = neoClient.SaveTransactions(transactions)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Time to save txids: %v\n", time.Since(start))

	fmt.Printf("%d transactions added successfully.\n", len(transactions))

	startChain := time.Now()
	for i := 0; i < chainCount; i++ {
		chainLength := mathRand.Intn(maxChainLength-1) + 1
		chained, err := generateChain(chainLength)

		if err != nil {
			log.Fatalf("Error generating chain: %s", err.Error())
		}

		for _, tx := range chained {
			if tx.Inputs != nil {
				txArray = append(txArray, tx.Hash.String())
			}
		}

		err = neoClient.SaveTransactions(chained)
		if err != nil {
			t.Error(err)
		}
	}
	fmt.Println("Chained transactions added successfully.")
	fmt.Printf("Time to save chain: %v\n", time.Since(startChain))
	// now save some subtrees
	subtreeSize := len(txArray) / subtreeCount
	subtrees := make([][]string, subtreeCount)

	start = time.Now()
	r := mathRand.New(mathRand.NewSource(time.Now().UnixNano()))
	for i := range subtrees {
		for j := 0; j < subtreeSize; j++ {
			// Pick a random transaction from the transactions array
			randomIndex := r.Intn(len(txArray))
			subtrees[i] = append(subtrees[i], txArray[randomIndex])
		}
	}
	fmt.Printf("Time to create subtrees chain: %v\n", time.Since(start))

	start = time.Now()
	for _, subtree := range subtrees {
		subtreeHash := generateRandomHash().String()
		subtreeHashes = append(subtreeHashes, subtreeHash)
		err = neoClient.SaveSubtree(subtreeHash, subtree)
		if err != nil {
			t.Error(err)
		}
	}
	fmt.Printf("Time to save subtrees chain: %v\n", time.Since(start))

	start = time.Now()
	// now mine a block
	bh, err := mineBlock(txArray)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("Time to mine block: %v\n", time.Since(start))

	start = time.Now()
	// now get the transactions in the bloack
	tib, err := neoClient.GetChainedTransactionsInBlock(bh.Hash.String())
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%d chained transactions in block\n", len(tib))
	fmt.Printf("Time to get chained transactions in block: %v\n", time.Since(start))

	start = time.Now()
	txs, err := neoClient.GetTransactionsNotInSubtrees(subtreeHashes)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%d transactions not in subtrees\n", len(txs))
	fmt.Printf("Time to get transactions not in subtrees: %v\n", time.Since(start))
}
