package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	mathRand "math/rand"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
)

const (
// unchainedTransactionCount = 1_000_000
// maxChainLength = 10
// chainCount     = 200
// subtreeCount   = 3
// blockCount     = 3
)

// var xunchainedTransactionCount int
// var xmaxChainLength int
// var xchainCount int
// var xsubtreeCount int
// var xblockCount int

var txArray []string
var subtreeHashes []string
var url string
var username string
var password string

func main() {
	// Define command-line arguments
	// use like  -txcount=10000
	utc := flag.Int("txCount", 1_000_000, "unchained transaction count")
	mcl := flag.Int("maxChainLength", 10, "maximum chain length")
	cc := flag.Int("chainCount", 200, "chain count")
	sc := flag.Int("subtreeCount", 3, "subtree count")

	u := flag.String("url", "neo4j://localhost:7687", "neo4j URL")
	us := flag.String("username", "neo4j", "neo4j username")
	pwd := flag.String("password", "localneo", "neo4j password")

	// Parse command-line arguments
	flag.Parse()

	unchainedTransactionCount := *utc
	maxChainLength := *mcl
	chainCount := *cc
	subtreeCount := *sc

	url = *u
	username = *us
	password = *pwd

	fmt.Println("unchainedTransactionCount:", unchainedTransactionCount)
	fmt.Println("maxChainLength:", maxChainLength)
	fmt.Println("chainCount:", chainCount)
	fmt.Println("subtreeCount:", subtreeCount)

	txArray := make([]string, 0)

	neoClient, err := NewNeoClient(url, username, password)
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
		fmt.Println(err)
		return
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
			fmt.Println(err)
			return
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
			fmt.Println(err)
			return
		}
	}
	fmt.Printf("Time to save subtrees chain: %v\n", time.Since(start))

	start = time.Now()
	// now mine a block
	bh, err := mineBlock(txArray)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Time to mine block: %v\n", time.Since(start))

	start = time.Now()
	// now get the transactions in the bloack
	tib, err := neoClient.GetChainedTransactionsInBlock(bh.Hash.String())
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%d chained transactions in block\n", len(tib))
	fmt.Printf("Time to get chained transactions in block: %v\n", time.Since(start))

	start = time.Now()
	txs, err := neoClient.GetTransactionsNotInSubtrees(subtreeHashes)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%d transactions not in subtrees\n", len(txs))
	fmt.Printf("Time to get transactions not in subtrees: %v\n", time.Since(start))
}

func mineBlock(txs []string) (*BlockHeader, error) {

	neoClient, err := NewNeoClient(url, username, password)
	if err != nil {
		log.Fatalf("Error creating NeoClient: %s", err.Error())
	}
	defer neoClient.Close()

	// get the latest block in the db
	b, err := neoClient.GetBestBlockheader()
	if err != nil {
		return nil, err
	}
	bh := &BlockHeader{}
	ch, err := chainhash.NewHashFromStr("")
	if err != nil {
		return nil, err
	}
	// could be first block
	if b == nil || b.Hash == nil {
		bh.Hash = generateRandomHash()
		bh.PreviousHash = ch
		bh.Height = 1
		bh.Chainwork = 10101010
	} else {
		bh.Hash = generateRandomHash()
		bh.PreviousHash = b.Hash
		bh.Height = b.Height + 1
		bh.Chainwork = b.Chainwork

	}
	// create a new block

	// save block

	err = neoClient.SaveBlock(bh, txArray)
	if err != nil {
		return nil, err
	}
	return bh, nil
}

func generateChain(chainLength int) ([]*TxInfo, error) {
	transactions := make([]*TxInfo, chainLength)

	// Start with a random initial transaction that has no inputs and one output
	firstTxHash := generateRandomHash()
	firstInputHash := generateRandomHash()
	transactions[0] = generateRandomChainedTxInfo(firstTxHash, []*Utxo{generateRandomUtxo(firstInputHash, 1)}, []*Utxo{generateRandomUtxo(firstTxHash, 1)})

	// Generate a chain of transactions where each transaction spends the outputs of the previous one
	for i := 1; i < chainLength; i++ {
		txHash := generateRandomHash()
		inputs := transactions[i-1].Outputs
		outputs := []*Utxo{generateRandomUtxo(txHash, 1)} // Each transaction generates one new output
		transactions[i] = generateRandomChainedTxInfo(txHash, inputs, outputs)
	}

	// Now `transactions` contains a chain of 100,000 transactions where each transaction
	// spends the outputs of the previous one. You can use these to populate your Neo4j database.

	return transactions, nil
}

func generateRandomChainedTxInfo(hash *chainhash.Hash, inputs, outputs []*Utxo) *TxInfo {
	return &TxInfo{
		Hash:    hash,
		Fee:     1,
		Inputs:  inputs,
		Outputs: outputs,
	}
}

func generateRandomTxInfo(numInputs, numOutputs int) *TxInfo {

	hash := generateRandomHash()
	inputHash := generateRandomHash()

	inputs := make([]*Utxo, numInputs)
	for i := range inputs {
		inputs[i] = generateRandomUtxo(inputHash, 1)
	}

	outputs := make([]*Utxo, numOutputs)
	for i := range outputs {
		outputs[i] = generateRandomUtxo(hash, 1)
	}

	return &TxInfo{
		Hash:    hash,
		Fee:     1,
		Inputs:  inputs,
		Outputs: outputs,
	}
}

func generateRandomUtxo(hash *chainhash.Hash, numVouts uint32) *Utxo {
	vouts := make([]uint32, numVouts)
	for i := range vouts {
		vouts[i] = uint32(i)
	}

	script := make([]byte, 64)
	_, _ = rand.Read(script)

	return &Utxo{
		Hash:     hash,
		Vout:     vouts,
		Satoshis: 10000, // Or some other random value
		Script:   script,
	}
}

func generateRandomHash() *chainhash.Hash {
	bytes := make([]byte, 32)
	_, _ = rand.Read(bytes)
	c, _ := chainhash.NewHash(bytes)
	return c
}
