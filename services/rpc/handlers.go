package rpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/txscript"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/p2p"
	"github.com/bitcoin-sv/ubsv/services/rpc/bsvjson"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/segmentio/encoding/json"
)

// handleGetBlock implements the getblock command.
func handleGetBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlock),
		tracing.WithLogMessage(s.logger, "[handleGetBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetBlockCmd)

	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlock(ctx, ch)
	if err != nil {
		return nil, err
	}

	return blockToJSON(ctx, b, *c.Verbosity, s)
}

// handleGetBlockByHeight implements the getblockbyheight command.
func handleGetBlockByHeight(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetBlockByHeight",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockByHeight),
		tracing.WithLogMessage(s.logger, "[handleGetBlockByHeight] called"),
	)

	defer deferFn()

	c := cmd.(*bsvjson.GetBlockByHeightCmd)

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlockByHeight(ctx, c.Height)
	if err != nil {
		return nil, err
	}

	return blockToJSON(ctx, b, *c.Verbosity, s)
}

// handleGetBlockHash implements the getblockhash command.
func handleGetBlockHash(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetBlockHash",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockHash),
		tracing.WithLogMessage(s.logger, "[handleGetBlockHash] called"),
	)

	defer deferFn()

	c := cmd.(*bsvjson.GetBlockHashCmd)

	// Load the raw block bytes from the database.
	b, err := s.blockchainClient.GetBlockByHeight(ctx, uint32(c.Index))
	if err != nil {
		return nil, err
	}

	return b.Hash().String(), nil
}

// handleGetBlockHash implements the getblockheader command.
func handleGetBlockHeader(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetBlockHeader",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBlockHeader),
		tracing.WithLogMessage(s.logger, "[handleGetBlockHeader] called"),
	)

	defer deferFn()

	c := cmd.(*bsvjson.GetBlockHeaderCmd)

	ch, err := chainhash.NewHashFromStr(c.Hash)
	if err != nil {
		return nil, rpcDecodeHexError(c.Hash)
	}
	// Load the raw block bytes from the database.
	b, meta, err := s.blockchainClient.GetBlockHeader(ctx, ch)
	if err != nil {
		return nil, err
	}

	if *c.Verbose {
		diff := b.Bits.CalculateDifficulty()
		diffFloat, _ := diff.Float64()
		headerReply := &bsvjson.GetBlockHeaderVerboseResult{
			Hash:         b.Hash().String(),
			Version:      int32(b.Version),
			VersionHex:   fmt.Sprintf("%08x", b.Version),
			PreviousHash: b.HashPrevBlock.String(),
			Nonce:        uint64(b.Nonce),
			Time:         int64(b.Timestamp),
			Bits:         b.Bits.String(),
			Difficulty:   diffFloat,
			MerkleRoot:   b.HashMerkleRoot.String(),
			// Confirmations: int64(1 + bestBlockMeta.Height - meta.Height),
			Height: int32(meta.Height),
		}

		return headerReply, nil
	}

	return fmt.Sprintf("%x", b.Bytes()), nil
}

func blockToJSON(ctx context.Context, b *model.Block, verbosity uint32, s *RPCServer) (interface{}, error) {
	if b == nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCBlockNotFound,
			Message: "Block not found",
		}
	}

	// When the verbosity value set to 0, simply return the serialized block
	// as a hex-encoded string.
	blkBytes, err := b.Bytes()
	if err != nil {
		return nil, err
	}

	if verbosity == 0 {
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

	baseBlockReply := &bsvjson.GetBlockBaseVerboseResult{
		Hash:          b.Hash().String(),
		Version:       int32(b.Header.Version), // nolint:gosec
		VersionHex:    fmt.Sprintf("%08x", b.Header.Version),
		MerkleRoot:    b.Header.HashMerkleRoot.String(),
		PreviousHash:  b.Header.HashPrevBlock.String(),
		Nonce:         b.Header.Nonce,
		Time:          int64(b.Header.Timestamp),
		Confirmations: int64(1 + bestBlockMeta.Height - b.Height),
		Height:        int64(b.Height),
		Size:          int32(len(blkBytes)), // nolint:gosec
		Bits:          b.Header.Bits.String(),
		Difficulty:    diff,
		NextHash:      nextBlock.Hash().String(),
	}

	// TODO: we can't add the txs to the block as there could be too many.
	// A breaking change would be to add the subtrees.

	// If verbose level does not match 0 or 1
	// we can consider it 2 (current bitcoin core behavior)
	if verbosity == 1 {
		// 	transactions := blk.Transactions()
		// 	txNames := make([]string, len(transactions))
		// 	for i, tx := range transactions {
		// 		txNames[i] = tx.Hash().String()
		// 	}

		// 	blockReply = bsvjson.GetBlockVerboseResult{
		// 		GetBlockBaseVerboseResult: baseBlockReply,

		// 		Tx: txNames,
		// 	}
		// } else {
		// 	txns := blk.Transactions()
		// 	rawTxns := make([]bsvjson.TxRawResult, len(txns))
		// 	for i, tx := range txns {
		// 		rawTxn, err := createTxRawResult(params, tx.MsgTx(),
		// 			tx.Hash().String(), blockHeader, hash.String(),
		// 			blockHeight, best.Height)
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		rawTxns[i] = *rawTxn
		// 	}

		blockReply = bsvjson.GetBlockVerboseTxResult{
			GetBlockBaseVerboseResult: baseBlockReply,

			// Tx: rawTxns,
		}
	}

	return blockReply, nil
}

// handleGetBestBlockHash implements the getbestblockhash command.
func handleGetBestBlockHash(ctx context.Context, s *RPCServer, _ interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetBestBlockHash",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetBestBlockHash),
		tracing.WithLogMessage(s.logger, "[handleGetBestBlockHash] called"),
	)
	defer deferFn()

	bh, _, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	hash := bh.Hash()

	return hash.String(), nil
}

// handleGetRawTransaction implements the getrawtransaction command.
// TODO: this is not implemented correctly, it should return the transaction in the same format as bitcoind
func handleGetRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "handleGetRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleGetRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.GetRawTransactionCmd)

	if s.assetHttpURL == nil {
		return nil, errors.NewConfigurationError("asset_httpURL is not set")
	}

	fullURL := s.assetHttpURL.ResolveReference(&url.URL{Path: fmt.Sprintf("/api/v1/tx/%s/hex", c.Txid)})

	// Set up HTTP client with timeouts
	client := &http.Client{
		Timeout: time.Second * 10,
	}

	// Send an HTTP GET request to the URL
	resp, err := client.Get(fullURL.String())
	if err != nil {
		return nil, errors.NewServiceError("Error: " + err.Error())
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		return nil, errors.NewServiceError(fmt.Sprintf("Error: Unexpected status code %d", resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)

	return body, nil
}

// handleCreateRawTransaction handles createrawtransaction commands.
func handleCreateRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "handleCreateRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleCreateRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleCreateRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.CreateRawTransactionCmd)

	// Validate the locktime, if given.
	if c.LockTime != nil &&
		(*c.LockTime < 0 || *c.LockTime > int64(wire.MaxTxInSequenceNum)) {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
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
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCType,
				Message: "Invalid amount",
			}
		}

		// Decode the provided address.
		addr, err := bsvutil.DecodeAddress(encodedAddr, &chaincfg.MainNetParams)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidAddressOrKey,
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
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidAddressOrKey,
				Message: `Invalid address or key`,
			}
		}

		if !addr.IsForNet(&chaincfg.MainNetParams) {
			return nil, &bsvjson.RPCError{
				Code: bsvjson.ErrRPCInvalidAddressOrKey,
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
		mtx.LockTime = uint32(*c.LockTime) // nolint:gosec
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
func handleSendRawTransaction(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "handleSendRawTransaction",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSendRawTransaction),
		tracing.WithLogMessage(s.logger, "[handleSendRawTransaction] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.SendRawTransactionCmd)
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
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCDeserialization,
			Message: "TX rejected: " + err.Error(),
		}
	}

	s.logger.Debugf("tx to send: %v", tx)

	d, err := distributor.NewDistributor(context.Background(), s.logger)
	if err != nil {
		return nil, errors.NewServiceError("could not create distributor", err)
	}

	res, err := d.SendTransaction(context.Background(), tx)
	if err != nil {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "TX rejected: " + err.Error(),
		}
	}

	return res, nil
}

// handleGenerate handles generate commands.
func handleGenerate(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	c := cmd.(*bsvjson.GenerateCmd)
	_, _, deferFn := tracing.StartTracing(ctx, "handleGenerate",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGenerate),
		tracing.WithLogMessage(s.logger, "[handleGenerate] called for %d blocks", c.NumBlocks),
	)

	defer deferFn()

	// Respond with an error if there's virtually 0 chance of mining a block
	// with the CPU.
	if !s.chainParams.GenerateSupported {
		return nil, &bsvjson.RPCError{
			Code: bsvjson.ErrRPCDifficulty,
			Message: fmt.Sprintf("No support for `generate` on "+
				"the current network, %s, as it's unlikely to "+
				"be possible to mine a block with the CPU.",
				s.chainParams.Net),
		}
	}

	if c.NumBlocks == 0 {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInternal.Code,
			Message: "Please request a nonzero number of blocks to generate.",
		}
	}

	minerHTTPPort, ok := gocore.Config().GetInt("MINER_HTTP_PORT")
	if !ok {
		s.logger.Warnf("MINER_HTTP_PORT not set in config")

		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCMisc,
			Message: "Can't contact miner",
		}
	}

	if minerHTTPPort < 0 || minerHTTPPort > 65535 {
		return nil, errors.NewInvalidArgumentError("Invalid port number: %d", minerHTTPPort)
	}

	if c.NumBlocks <= 0 {
		return nil, errors.NewInvalidArgumentError("Invalid number of blocks: %d", c.NumBlocks)
	}

	// causes lint:gosec error G107: Potential HTTP request made with variable url (gosec)
	// url := fmt.Sprintf("http://localhost:%d/mine?blocks=%d", minerHTTPPort, c.NumBlocks)

	// Construct URL using net/url
	u := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("localhost:%d", minerHTTPPort),
		Path:   "mine",
	}
	q := u.Query()
	q.Set("blocks", fmt.Sprintf("%d", c.NumBlocks))
	u.RawQuery = q.Encode()

	// Send the GET request
	response, err := http.Get(u.String())
	if err != nil {
		s.logger.Errorf("The HTTP request failed with error %s\n", err)

		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCMisc,
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
	// 	return nil, &bsvjson.RPCError{
	// 		Code:    bsvjson.ErrRPCInternal.Code,
	// 		Message: herr.Error(),
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

func handleGetMiningCandidate(ctx context.Context, s *RPCServer, _ interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetMiningCandidate",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetMiningCandidate),
		tracing.WithLogMessage(s.logger, "[handleGetMiningCandidate] called"),
	)
	defer deferFn()

	mc, err := s.blockAssemblyClient.GetMiningCandidate(ctx)
	if err != nil {
		return nil, err
	}

	// Create a map to hold the fields as JSON
	pb := chainhash.Hash{}

	err = pb.SetBytes(mc.PreviousHash)
	if err != nil {
		return nil, err
	}

	reversedBits := bt.ReverseBytes(mc.NBits)

	merkleProofStrings := make([]string, len(mc.MerkleProof))

	for _, hash := range mc.MerkleProof { //
		merkleProofStrings = append(merkleProofStrings, hex.EncodeToString(hash))
	}

	jsonMap := map[string]interface{}{
		"id":            hex.EncodeToString(mc.Id),
		"prevhash":      pb.String(),
		"coinbaseValue": mc.CoinbaseValue,
		"version":       mc.Version,
		"nBits":         hex.EncodeToString(reversedBits),
		"time":          mc.Time,
		"height":        mc.Height,
		"merkleProof":   merkleProofStrings,
	}

	return jsonMap, nil
}

func handleGetpeerinfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetpeerinfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetpeerinfo),
		tracing.WithLogMessage(s.logger, "[handleGetpeerinfo] called"),
	)
	defer deferFn()

	peerCount := 0
	// get legacy peer info
	legacyPeerInfo, err := s.peerClient.GetPeers(ctx)
	if err != nil {
		// not critical -legacy service may not be running, so log as info
		s.logger.Infof("error getting legacy peer info: %v", err)
	} else {
		peerCount += len(legacyPeerInfo.Peers)
	}

	// get new peer info
	newPeerInfo, err := s.p2pClient.GetPeers(ctx)
	if err != nil {
		s.logger.Errorf("error getting new peer info: %v", err)
	} else {
		peerCount += len(newPeerInfo.Peers)
	}

	for _, np := range newPeerInfo.Peers {
		s.logger.Debugf("new peer: %v", np)
	}

	infos := make([]*bsvjson.GetPeerInfoResult, 0, peerCount)

	if legacyPeerInfo != nil {
		for _, p := range legacyPeerInfo.Peers {
			info := &bsvjson.GetPeerInfoResult{
				ID:        p.Id,
				Addr:      p.Addr,
				AddrLocal: p.AddrLocal,
				// Services:       fmt.Sprintf("%08d", uint64(statsSnap.Services)),
				ServicesStr: p.Services,
				// RelayTxes:      !p.IsTxRelayDisabled(),
				LastSend:       p.LastSend,
				LastRecv:       p.LastRecv,
				BytesSent:      p.BytesSent,
				BytesRecv:      p.BytesReceived,
				ConnTime:       p.ConnTime,
				PingTime:       float64(p.PingTime),
				TimeOffset:     p.TimeOffset,
				Version:        p.Version,
				SubVer:         p.SubVer,
				Inbound:        p.Inbound,
				StartingHeight: p.StartingHeight,
				CurrentHeight:  p.CurrentHeight,
				BanScore:       p.Banscore,
				Whitelisted:    p.Whitelisted,
				FeeFilter:      p.FeeFilter,
				// SyncNode:       p.ID == syncPeerID,
			}
			// if p.ToPeer().LastPingNonce() != 0 {
			// 	wait := float64(time.Since(p.LastPingTime).Nanoseconds())
			// 	// We actually want microseconds.
			// 	info.PingWait = wait / 1000
			// }
			infos = append(infos, info)
		}
	}

	if newPeerInfo != nil {
		for _, p := range newPeerInfo.Peers {
			info := &bsvjson.GetPeerInfoResult{
				// ID:        p.Id,
				Addr: p.Addr,
				// AddrLocal: p.AddrLocal,
			}
			infos = append(infos, info)
		}
	}

	// return peerInfo, nil
	return infos, nil
}

func handleGetDifficulty(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetDifficulty",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetDifficulty),
		tracing.WithLogMessage(s.logger, "[handleGetDifficulty] called"),
	)
	defer deferFn()

	difficulty, err := s.blockAssemblyClient.GetCurrentDifficulty(ctx)
	if err != nil {
		return nil, err
	}

	return difficulty, nil
}

func handleGetblockchaininfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetblockchaininfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetblockchaininfo),
		tracing.WithLogMessage(s.logger, "[handleGetblockchaininfo] called"),
	)
	defer deferFn()

	bestBlockHeader, bestBlockMeta, err := s.blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		s.logger.Errorf("error getting best block header: %v", err)
	}

	jsonMap := map[string]interface{}{
		"chain":                s.chainParams.Name,
		"blocks":               bestBlockMeta.Height,
		"headers":              863341,
		"bestblockhash":        bestBlockHeader.Hash().String(),
		"difficulty":           bestBlockHeader.Bits.CalculateDifficulty(),
		"mediantime":           0,
		"verificationprogress": 0,
		"chainwork":            "",
		"pruned":               false, // the minimum relay fee for non-free transactions in BSV/KB
		"softforks":            []interface{}{},
	}

	return jsonMap, nil
}

// handleGetInfo returns a JSON object containing various state info.
func handleGetInfo(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleGetInfo",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleGetinfo),
		tracing.WithLogMessage(s.logger, "[handleGetInfo] called"),
	)
	defer deferFn()

	height, _, err := s.blockchainClient.GetBestHeightAndTime(ctx)
	if err != nil {
		s.logger.Errorf("error getting best height and time: %v", err)

		height = 0
	}

	jsonMap := map[string]interface{}{
		"version":         1,                                  // the version of the server
		"protocolversion": wire.ProtocolVersion,               // the latest supported protocol version
		"blocks":          height,                             // the number of blocks processed
		"timeoffset":      1,                                  // the time offset
		"connections":     1,                                  // the number of connected peers
		"proxy":           "host:port",                        // the proxy used by the server
		"difficulty":      1,                                  // the current target difficulty
		"testnet":         s.chainParams.Net == wire.TestNet3, // whether or not server is using testnet
		"stn":             s.chainParams.Net == wire.STN,      // whether or not server is using stn
		"relayfee":        100,                                // the minimum relay fee for non-free transactions in BSV/KB

	}

	return jsonMap, nil
}

func handleSubmitMiningSolution(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleSubmitMiningSolution",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSubmitMiningSolution),
		tracing.WithLogMessage(s.logger, "[handleSubmitMiningSolution] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.SubmitMiningSolutionCmd)

	ms := &model.MiningSolution{}

	err := json.Unmarshal([]byte(c.JSONString), ms)
	if err != nil {
		return nil, err
	}

	s.logger.Debugf("in handleSubmitMiningSolution: ms: %+v", ms)

	err = s.blockAssemblyClient.SubmitMiningSolution(ctx, ms)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// handleInvalidateBlock implements the invalidateblock command.
func handleInvalidateBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleInvalidateBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleInvalidateBlock),
		tracing.WithLogMessage(s.logger, "[handleInvalidateBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.InvalidateBlockCmd)

	ch, err := chainhash.NewHashFromStr(c.BlockHash)
	if err != nil {
		return nil, rpcDecodeHexError(c.BlockHash)
	}

	err = s.blockchainClient.InvalidateBlock(ctx, ch)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

// handleReconsiderBlock implements the reconsiderblock command.
func handleReconsiderBlock(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "handleReconsiderBlock",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleReconsiderBlock),
		tracing.WithLogMessage(s.logger, "[handleReconsiderBlock] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.ReconsiderBlockCmd)

	ch, err := chainhash.NewHashFromStr(c.BlockHash)
	if err != nil {
		return nil, rpcDecodeHexError(c.BlockHash)
	}

	// Load the raw block bytes from the database.
	err = s.blockchainClient.RevalidateBlock(ctx, ch)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func handleHelp(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "handleHelp",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleHelp),
		tracing.WithLogMessage(s.logger, "[handleHelp] called"),
	)
	defer deferFn()

	c := cmd.(*bsvjson.HelpCmd)

	// Provide a usage overview of all commands when no specific command
	// was specified.
	var command string
	if c.Command != nil {
		command = *c.Command
	}

	if command == "" {
		usage, err := s.helpCacher.rpcUsage()
		if err != nil {
			context := "Failed to generate RPC usage"
			return nil, internalRPCError(err.Error(), context)
		}

		return usage, nil
	}

	// Check that the command asked for is supported and implemented.  Only
	// search the main list of handlers since help should not be provided
	// for commands that are unimplemented or related to wallet
	// functionality.
	if _, ok := rpcHandlers[command]; !ok {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Unknown command: " + command,
		}
	}

	// Get the help for the command.
	help, err := s.helpCacher.rpcMethodHelp(command)
	if err != nil {
		context := "Failed to generate help"
		return nil, internalRPCError(err.Error(), context)
	}

	return help, nil
}

func handleSetBan(ctx context.Context, s *RPCServer, cmd interface{}, _ <-chan struct{}) (interface{}, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "handleSetBan",
		tracing.WithParentStat(RPCStat),
		tracing.WithHistogram(prometheusHandleSetBan),
		tracing.WithLogMessage(s.logger, "[handleSetBan] called"),
	)
	defer deferFn()

	banList := p2p.NewBanList(s.logger)

	var subnet *net.IPNet

	var err error

	c := cmd.(*bsvjson.SetBanCmd)

	s.logger.Debugf("in handleSetBan: c: %+v", c)

	if c.IPOrSubnet == "" {
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "IPOrSubnet is required",
		}
	}

	// Handle the command
	switch c.Command {
	case "add":
		var expirationTime time.Time
		if c.Absolute {
			expirationTime = time.Unix(c.BanTime, 0)
		} else {
			expirationTime = time.Now().Add(time.Duration(c.BanTime) * time.Second)
		}

		// If BanTime is 0, use a default ban time (e.g., 24 hours)
		if c.BanTime == 0 {
			expirationTime = time.Now().Add(24 * time.Hour)
		}

		err = banList.Add(c.IPOrSubnet, expirationTime)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidParameter,
				Message: "Failed to add ban",
			}
		}

		s.logger.Debugf("Added ban for %s until %v", subnet.String(), expirationTime)
	case "remove":
		err = banList.Remove(c.IPOrSubnet)
		if err != nil {
			return nil, &bsvjson.RPCError{
				Code:    bsvjson.ErrRPCInvalidParameter,
				Message: "Failed to remove ban",
			}
		}

		s.logger.Debugf("Removed ban for %s", c.IPOrSubnet)
	default:
		return nil, &bsvjson.RPCError{
			Code:    bsvjson.ErrRPCInvalidParameter,
			Message: "Invalid command. Must be 'add' or 'remove'.",
		}
	}

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
