package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/bitcoinsv/bsvd/btcjson"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/txscript"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/util/distributor"
)

// handleGetBlock implements the getblock command.
func handleGetBlock(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	ctx := context.Background()
	c := cmd.(*btcjson.GetBlockCmd)
	s.logger.Debugf("handleGetBlock %s", c.Hash)
	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}
	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlock(ctx, ch)
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

	// get best block header
	_, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	// Get next block hash unless there are none.
	nextBlock, err := s.blockchainClient.GetBlockByHeight(ctx, b.Height+1)
	if err != nil {
		return nil, err
	}

	var (
		blockReply interface{}
	// 	params      = s.cfg.ChainParams
	// 	blockHeader = &blk.MsgBlock().Header
	)
	diff, _ := b.Header.Bits.CalculateDifficulty().Float64()

	baseBlockReply := &btcjson.GetBlockBaseVerboseResult{
		Hash:          c.Hash,
		Version:       int32(b.Header.Version),
		VersionHex:    fmt.Sprintf("%08x", b.Header.Version),
		MerkleRoot:    b.Header.HashMerkleRoot.String(),
		PreviousHash:  b.Header.HashPrevBlock.String(),
		Nonce:         b.Header.Nonce,
		Time:          int64(b.Header.Timestamp),
		Confirmations: int64(1 + bestBlockMeta.Height - b.Height),
		Height:        int64(b.Height),
		Size:          int32(len(blkBytes)),
		Bits:          b.Header.Bits.String(),
		Difficulty:    diff,
		NextHash:      string(nextBlock.Hash().String()),
	}

	// TODO: we can't add the txs to the block as there could be too many.
	// A breaking change would be to add the subtrees.

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

// handleCreateRawTransaction handles createrawtransaction commands.
func handleCreateRawTransaction(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	c := cmd.(*btcjson.CreateRawTransactionCmd)

	// Validate the locktime, if given.
	if c.LockTime != nil &&
		(*c.LockTime < 0 || *c.LockTime > int64(wire.MaxTxInSequenceNum)) {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCInvalidParameter,
			Message: "Locktime out of range",
		}
	}

	// Add all transaction inputs to a new transaction after performing
	// some validity checks.
	mtx := wire.NewMsgTx(wire.TxVersion)
	for _, input := range c.Inputs {
		txHash, err := chainhash.NewHashFromStr(input.Txid)
		if err != nil {
			return nil, rpcDecodeHexError(input.Txid)
		}

		prevOut := wire.NewOutPoint(txHash, input.Vout)
		txIn := wire.NewTxIn(prevOut, []byte{})
		if c.LockTime != nil && *c.LockTime != 0 {
			txIn.Sequence = wire.MaxTxInSequenceNum - 1
		}
		mtx.AddTxIn(txIn)
	}

	// Add all transaction outputs to the transaction after performing
	// some validity checks.
	// params := s.cfg.ChainParams
	for encodedAddr, amount := range c.Amounts {
		// Ensure amount is in the valid range for monetary amounts.
		if amount <= 0 || amount > bsvutil.MaxSatoshi {
			return nil, &btcjson.RPCError{
				Code:    btcjson.ErrRPCType,
				Message: "Invalid amount",
			}
		}

		// Decode the provided address.
		addr, err := bsvutil.DecodeAddress(encodedAddr, &chaincfg.MainNetParams)
		if err != nil {
			return nil, &btcjson.RPCError{
				Code:    btcjson.ErrRPCInvalidAddressOrKey,
				Message: "Invalid address or key: " + err.Error(),
			}
		}

		// Ensure the address is one of the supported types and that
		// the network encoded with the address matches the network the
		// server is currently on.
		switch addr.(type) {
		case *bsvutil.AddressPubKeyHash:
		case *bsvutil.AddressScriptHash:
		case *bsvutil.LegacyAddressPubKeyHash: // TODO: support legacy addresses?
		default:
			return nil, &btcjson.RPCError{
				Code:    btcjson.ErrRPCInvalidAddressOrKey,
				Message: `Invalid address or key`,
			}
		}
		if !addr.IsForNet(&chaincfg.MainNetParams) {
			return nil, &btcjson.RPCError{
				Code: btcjson.ErrRPCInvalidAddressOrKey,
				Message: "Invalid address: " + encodedAddr +
					" is for the wrong network",
			}
		}

		// Create a new script which pays to the provided address.
		pkScript, err := txscript.PayToAddrScript(addr)
		if err != nil {
			context := "Failed to generate pay-to-address script"
			return nil, internalRPCError(err.Error(), context)
		}

		// Convert the amount to satoshi.
		satoshi, err := bsvutil.NewAmount(amount)
		if err != nil {
			context := "Failed to convert amount"
			return nil, internalRPCError(err.Error(), context)
		}

		txOut := wire.NewTxOut(int64(satoshi), pkScript)
		mtx.AddTxOut(txOut)
	}

	// Set the Locktime, if given.
	if c.LockTime != nil {
		mtx.LockTime = uint32(*c.LockTime)
	}

	// Return the serialized and hex-encoded transaction.  Note that this
	// is intentionally not directly returning because the first return
	// value is a string and it would result in returning an empty string to
	// the client instead of nothing (nil) in the case of an error.
	mtxHex, err := messageToHex(mtx)
	if err != nil {
		return nil, err
	}
	return mtxHex, nil
}

// handleSendRawTransaction implements the sendrawtransaction command.
func handleSendRawTransaction(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {
	c := cmd.(*btcjson.SendRawTransactionCmd)
	// Deserialize and send off to tx relay
	hexStr := c.HexTx
	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr
	}
	serializedTx, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, rpcDecodeHexError(hexStr)
	}

	// Use 0 for the tag to represent local node.
	tx, err := bt.NewTxFromBytes(serializedTx)
	if err != nil {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCDeserialization,
			Message: "TX rejected: " + err.Error(),
		}
	}
	s.logger.Debugf("tx to send: %v", tx)

	d, err := distributor.NewDistributor(s.logger)
	if err != nil {
		return nil, fmt.Errorf("could not create distributor: %v", err)
	}

	res, err := d.SendTransaction(context.Background(), tx)
	if err != nil {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCInvalidParameter,
			Message: "TX rejected: " + err.Error(),
		}
	}

	return res, nil

}

// handleGenerate handles generate commands.
func handleGenerate(s *rpcServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error) {

	c := cmd.(*btcjson.GenerateCmd)

	s.logger.Debugf("generate %d blocks", c.NumBlocks)

	if c.NumBlocks == 0 {
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCInternal.Code,
			Message: "Please request a nonzero number of blocks to generate.",
		}
	}

	minerHttpPort, ok := gocore.Config().GetInt("MINER_HTTP_PORT")
	if !ok {
		s.logger.Warnf("MINER_HTTP_PORT not set in config")
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCMisc,
			Message: "Can't contact miner",
		}
	}
	if minerHttpPort < 0 || minerHttpPort > 65535 {
		s.logger.Fatalf("Invalid port number: %d", minerHttpPort)
	}

	if c.NumBlocks < 0 {
		s.logger.Fatalf("Invalid number of blocks: %d", c.NumBlocks)
	}

	// causes lint:gosec error G107: Potential HTTP request made with variable url (gosec)
	// url := fmt.Sprintf("http://localhost:%d/mine?blocks=%d", minerHttpPort, c.NumBlocks)

	// Construct URL using net/url
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("localhost:%d", minerHttpPort),
		Path:   "mine",
	}
	q := u.Query()
	q.Set("blocks", fmt.Sprintf("%d", c.NumBlocks))
	u.RawQuery = q.Encode()

	// Send the GET request
	response, err := http.Get(u.String())
	if err != nil {
		s.logger.Errorf("The HTTP request failed with error %s\n", err)
		return nil, &btcjson.RPCError{
			Code:    btcjson.ErrRPCMisc,
			Message: "Can't contact miner",
		}
	}
	defer response.Body.Close() // Always close the response body

	// Read the response body
	data, _ := io.ReadAll(response.Body)

	// Print the response status and body
	s.logger.Debugf("Status Code:", response.StatusCode)
	s.logger.Debugf("Response Body: %s\n", data)

	// // Respond with an error if the client is requesting 0 blocks to be generated.

	// // Create a reply
	// reply := make([]string, c.NumBlocks)

	// blockHashes, err := s.cfg.CPUMiner.GenerateNBlocks(c.NumBlocks)
	// if err != nil {
	// 	return nil, &btcjson.RPCError{
	// 		Code:    btcjson.ErrRPCInternal.Code,
	// 		Message: err.Error(),
	// 	}
	// }

	// // Mine the correct number of blocks, assigning the hex representation of the
	// // hash of each one to its place in the reply.
	// for i, hash := range blockHashes {
	// 	reply[i] = hash.String()
	// }

	// return reply, nil
	return nil, nil
}

// messageToHex serializes a message to the wire protocol encoding using the
// latest protocol version and returns a hex-encoded string of the result.
func messageToHex(msg wire.Message) (string, error) {
	var buf bytes.Buffer
	if err := msg.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding); err != nil {
		context := fmt.Sprintf("Failed to encode msg of type %T", msg)
		return "", internalRPCError(err.Error(), context)
	}

	return hex.EncodeToString(buf.Bytes()), nil
}
