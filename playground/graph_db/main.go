package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"sync"

	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var groupSize = 10000

type Utxo struct {
	Hash     *chainhash.Hash
	Vout     []uint32
	Satoshis uint64
	Script   []byte
}

type TxInfo struct {
	Hash    *chainhash.Hash
	Fee     uint64
	Inputs  []*Utxo
	Outputs []*Utxo
}

type BlockHeader struct {
	Hash         *chainhash.Hash
	PreviousHash *chainhash.Hash
	Height       int64
	Chainwork    int64
}

type NeoClient struct {
	driver   neo4j.Driver
	session  neo4j.Session
	username string
	password string
	url      string
}

func NewNeoClient() (*NeoClient, error) {
	c := &NeoClient{}
	c.url = "neo4j://localhost:7687"
	c.username = "neo4j"
	c.password = "localneo"
	// Neo4j driver configuration
	var err error
	c.driver, err = neo4j.NewDriver(c.url, neo4j.BasicAuth(c.username, c.password, ""))
	if err != nil {
		log.Fatal(err)
	}

	// Start a new session
	c.session = c.driver.NewSession(neo4j.SessionConfig{})

	err = c.CreateIndex()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *NeoClient) Close() []error {
	var errors []error
	err := c.session.Close()
	errors = append(errors, err)
	err = c.driver.Close()
	errors = append(errors, err)
	return errors
}

func (c *NeoClient) CreateIndex() error {
	session := c.driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	_, err := session.Run(`CREATE INDEX tx_hash_index IF NOT EXISTS FOR (tx:Transaction) ON (tx.hash)`, nil)
	if err != nil {
		return err
	}

	_, err = session.Run("CREATE INDEX subtree_hash_index IF NOT EXISTS FOR (s:Subtree) ON (s.hash)", nil)
	if err != nil {
		return err
	}

	return nil

}

func (c *NeoClient) SaveTransactions(transactions []*TxInfo) error {
	query := `
					MERGE (tx:Transaction {hash: $txHash, fee: $fee})
					WITH tx
					UNWIND $inputs AS input
					MERGE (in:Utxo {vout: input.vout})<-[:GENERATES]-(:Transaction {hash: input.hash})
					MERGE (tx)-[:SPENDS]->(in)
					WITH tx
					UNWIND $outputs AS output
					MERGE (out:Utxo {vout: output.vout})<-[:GENERATES]-(tx)
					MERGE (tx)-[:GENERATES]->(out)
 				`

	// Start a new transaction
	// _, err := c.session.WriteTransaction(func(neoTx neo4j.Transaction) (interface{}, error) {

	// start
	var wg sync.WaitGroup

	for i := 0; i < len(transactions); i += groupSize {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {

				for j := i; j < i+groupSize && j < len(transactions); j++ {
					// define your parameters
					params := map[string]interface{}{
						"txHash": transactions[j].Hash.String(),
						"fee":    transactions[j].Fee,
						"inputs": []map[string]interface{}{
							{"vout": 0, "hash": transactions[j].Inputs[0].Hash.String(), "satoshis": transactions[j].Inputs[0].Satoshis, "script": hex.EncodeToString(transactions[j].Inputs[0].Script)},
						},
						"outputs": []map[string]interface{}{
							{"vout": 0, "hash": transactions[j].Outputs[0].Hash.String(), "satoshis": transactions[j].Outputs[0].Satoshis, "script": hex.EncodeToString(transactions[j].Outputs[0].Script)},
						},
					}

					_, err := tx.Run(query, params)
					if err != nil {
						return nil, err
					}

				} // for j := i; j < i+groupSize && j < len(transactions); j++
				return nil, nil
			}) // 	_, err := c.session.WriteTransaction

			if err != nil {
				log.Printf("Error writing transaction: %s", err.Error())
			}
		}(i) // go func(i int) {
	} // for i := 0; i < len(transactions); i += groupSize

	wg.Wait()

	return nil
}

func (c *NeoClient) SaveBlock(bh *BlockHeader, txa []string) error {

	query := `CREATE (b:Block {hash: $hash, height: $height, chainwork: $chainwork, previous_hash: $previous_hash})
						WITH b
						UNWIND $tx_hashes AS tx_hash
						MERGE (tx:Transaction {hash: tx_hash})
						MERGE (b)-[:CONTAINS]->(tx)
						`
	// Start a new transaction
	_, err := c.session.WriteTransaction(func(neoTx neo4j.Transaction) (interface{}, error) {
		// Parameters for the query
		params := map[string]interface{}{
			"hash":          bh.Hash.String(),
			"previous_hash": bh.PreviousHash.String(),
			"height":        bh.Height,
			"chainwork":     bh.Chainwork,
			"tx_hashes":     txa,
		}

		// Run the query
		_, err := neoTx.Run(query, params)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	if err != nil {
		return err
	}
	return nil
}

func (c *NeoClient) SaveSubtree(subtreeHash string, txa []string) error {

	query := `CREATE (b:Subtree {hash: $hash})
						WITH b
						UNWIND $tx_hashes AS tx_hash
						MERGE (tx:Transaction {hash: tx_hash})
						MERGE (b)-[:CONTAINS]->(tx)
						`
	// Start a new transaction
	_, err := c.session.WriteTransaction(func(neoTx neo4j.Transaction) (interface{}, error) {
		// Parameters for the query
		params := map[string]interface{}{
			"hash":      subtreeHash,
			"tx_hashes": txa,
		}

		// Run the query
		_, err := neoTx.Run(query, params)
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	if err != nil {
		return err
	}
	return nil
}
func (c *NeoClient) GetChainedTransactionsInBlock(blockHash string) ([]string, error) {
	query := `MATCH (b:Block)-[:CONTAINS]->(tx:Transaction)-[:SPENDS]->(:Utxo)<-[:GENERATES]-(:Transaction),
					(tx)-[:GENERATES]->(:Utxo)<-[:SPENDS]-(:Transaction)
					WHERE b.hash = $block_hash
					RETURN tx.hash`
	result, err := c.session.Run(query, map[string]interface{}{
		"block_hash": blockHash,
	})
	if err != nil {
		return nil, err
	}

	var txHashes []string

	for result.Next() {
		record := result.Record()
		if value, ok := record.Values[0].(string); ok {
			txHashes = append(txHashes, value)
		}
	}

	if err = result.Err(); err != nil {
		return txHashes, err
	}

	return txHashes, nil
}

func (c *NeoClient) GetTransactionsNotInSubtrees(subtreeHashes []string) ([]string, error) {

	query := `MATCH (s:Subtree)
						WHERE s.hash IN $subtreeHashes
						MATCH (tx:Transaction)
						WHERE NOT (s)-[:CONTAINS]->(tx)
						RETURN tx.hash`

	params := map[string]interface{}{"subtreeHashes": subtreeHashes}

	result, err := c.session.Run(query, params)
	if err != nil {
		return nil, err
	}

	var remainingTxs []string
	for result.Next() {
		record := result.Record()
		if value, ok := record.Values[0].(string); ok {
			remainingTxs = append(remainingTxs, value)
		}
		// populate other fields of Transaction struct if needed
	}

	if err = result.Err(); err != nil {
		return nil, err
	}

	return remainingTxs, nil
}
func (c *NeoClient) GetBestBlockheader() (bh *BlockHeader, err error) {
	bh = &BlockHeader{}
	query := `MATCH (b:Block)
						RETURN b
						ORDER BY b.height DESC
						LIMIT 1`

	result, err := c.session.Run(query, nil)
	if err != nil {
		return nil, err
	}

	if result.Next() {
		record := result.Record()

		node, ok := record.Get("b")
		if !ok {
			return nil, fmt.Errorf("can't get best block")
		}

		blockNode := node.(neo4j.Node)
		bh.Height, ok = blockNode.Props["height"].(int64)
		if !ok {
			return nil, fmt.Errorf("can't get height for best block")
		}

		bh.Chainwork, ok = blockNode.Props["chainwork"].(int64)
		if !ok {
			return nil, fmt.Errorf("can't get chainwork for best block")
		}

		h := blockNode.Props["hash"].(string)
		bh.Hash, err = chainhash.NewHashFromStr(h)

		if err != nil {
			return nil, fmt.Errorf("can't get hash for best block")
		}
		ph := blockNode.Props["previous_hash"].(string)
		bh.PreviousHash, err = chainhash.NewHashFromStr(ph)
		if err != nil {
			return nil, fmt.Errorf("can't get previous_hash for best block")
		}

	}
	return bh, nil

}
