package rpc

import (
	"context"
	"encoding/hex"

	"github.com/bitcoinsv/bsvd/btcjson"
	"github.com/libsv/go-bt/v2/chainhash"
)

// handleGetBlock implements the getblock command.
func handleGetBlock(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	c := cmd.(*btcjson.GetBlockCmd)
	s.logger.Debugf("handleGetBlock %s", c.Hash)
	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}
	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlock(context.Background(), ch)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCBlockNotFound,
			Message: "Block not found",
		}
	}

	// When the verbosity value set to 0, simply return the serialized block
	// as a hex-encoded string.
	blkBytes, err := b.Bytes()
	if err != nil {
		return nil, err
	}
	if *c.Verbosity == 0 {
		// Generate the JSON object and return it.
		return hex.EncodeToString(blkBytes), nil
	}

	// Get the block height from chain.

	// Get next block hash unless there are none.

	var (
		blockReply interface{}
	// 	params      = s.cfg.ChainParams
	// 	blockHeader = &blk.MsgBlock().Header
	)
	baseBlockReply := &btcjson.GetBlockBaseVerboseResult{
		Hash:    c.Hash,
		Version: int32(b.Header.Version),
		// 	VersionHex:    fmt.Sprintf("%08x", blockHeader.Version),
		MerkleRoot:   b.Header.HashMerkleRoot.String(),
		PreviousHash: b.Header.HashPrevBlock.String(),
		Nonce:        b.Header.Nonce,
		Time:         int64(b.Header.Timestamp),
		// 	Confirmations: int64(1 + best.Height - blockHeight),
		// 	Height:        int64(blockHeight),
		Size: int32(len(blkBytes)),
		Bits: b.Header.Bits.String(),
		// 	Difficulty:    getDifficultyRatio(blockHeader.Bits, params),
		// 	NextHash:      nextHashString,
	}

	// If verbose level does not match 0 or 1
	// we can consider it 2 (current bitcoin core behavior)
	if *c.Verbosity == 1 {
		// 	transactions := blk.Transactions()
		// 	txNames := make([]string, len(transactions))
		// 	for i, tx := range transactions {
		// 		txNames[i] = tx.Hash().String()
		// 	}

		// 	blockReply = btcjson.GetBlockVerboseResult{
		// 		GetBlockBaseVerboseResult: baseBlockReply,

		// 		Tx: txNames,
		// 	}
		// } else {
		// 	txns := blk.Transactions()
		// 	rawTxns := make([]btcjson.TxRawResult, len(txns))
		// 	for i, tx := range txns {
		// 		rawTxn, err := createTxRawResult(params, tx.MsgTx(),
		// 			tx.Hash().String(), blockHeader, hash.String(),
		// 			blockHeight, best.Height)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		rawTxns[i] = *rawTxn
		// 	}

		blockReply = btcjson.GetBlockVerboseTxResult{
			GetBlockBaseVerboseResult: baseBlockReply,

			// Tx: rawTxns,
		}
	}

	return blockReply, nil
}

// handleGetBestBlockHash implements the getbestblockhash command.
func handleGetBestBlockHash(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	bh, _, err := s.blockchainClient.GetBestBlockHeader(context.Background())
	if err != nil {
		if err != nil {
			return nil, err
		}
	}
	hash := bh.Hash()
	return hash.String(), nil
}
