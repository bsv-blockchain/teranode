// Copyright (c) 2014 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package bsvjson_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/bitcoin-sv/teranode/services/rpc/bsvjson"
)

// TestChainSvrCmds tests all of the chain server commands marshal and unmarshal
// into valid results include handling of optional fields being omitted in the
// marshalled command, while optional fields with defaults have the default
// assigned on unmarshalled commands.
func TestChainSvrCmds(t *testing.T) {
	t.Parallel()

	testID := int(1)
	tests := []struct {
		name         string
		newCmd       func() (interface{}, error)
		staticCmd    func() interface{}
		marshalled   string
		unmarshalled interface{}
	}{
		{
			name: "addnode",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("addnode", "127.0.0.1", bsvjson.ANRemove)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewAddNodeCmd("127.0.0.1", bsvjson.ANRemove)
			},
			marshalled:   `{"jsonrpc":"1.0","method":"addnode","params":["127.0.0.1","remove"],"id":1}`,
			unmarshalled: &bsvjson.AddNodeCmd{Addr: "127.0.0.1", SubCmd: bsvjson.ANRemove},
		},
		{
			name: "createrawtransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("createrawtransaction", `[{"txid":"123","vout":1}]`,
					`{"456":0.0123}`)
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.TransactionInput{
					{Txid: "123", Vout: 1},
				}
				amounts := map[string]float64{"456": .0123}
				return bsvjson.NewCreateRawTransactionCmd(txInputs, amounts, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"createrawtransaction","params":[[{"txid":"123","vout":1}],{"456":0.0123}],"id":1}`,
			unmarshalled: &bsvjson.CreateRawTransactionCmd{
				Inputs:  []bsvjson.TransactionInput{{Txid: "123", Vout: 1}},
				Amounts: map[string]float64{"456": .0123},
			},
		},
		{
			name: "createrawtransaction optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("createrawtransaction", `[{"txid":"123","vout":1}]`,
					`{"456":0.0123}`, int64(12312333333))
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.TransactionInput{
					{Txid: "123", Vout: 1},
				}
				amounts := map[string]float64{"456": .0123}
				return bsvjson.NewCreateRawTransactionCmd(txInputs, amounts, bsvjson.Int64(12312333333))
			},
			marshalled: `{"jsonrpc":"1.0","method":"createrawtransaction","params":[[{"txid":"123","vout":1}],{"456":0.0123},12312333333],"id":1}`,
			unmarshalled: &bsvjson.CreateRawTransactionCmd{
				Inputs:   []bsvjson.TransactionInput{{Txid: "123", Vout: 1}},
				Amounts:  map[string]float64{"456": .0123},
				LockTime: bsvjson.Int64(12312333333),
			},
		},

		{
			name: "decoderawtransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("decoderawtransaction", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewDecodeRawTransactionCmd("123")
			},
			marshalled:   `{"jsonrpc":"1.0","method":"decoderawtransaction","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.DecodeRawTransactionCmd{HexTx: "123"},
		},
		{
			name: "decodescript",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("decodescript", "00")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewDecodeScriptCmd("00")
			},
			marshalled:   `{"jsonrpc":"1.0","method":"decodescript","params":["00"],"id":1}`,
			unmarshalled: &bsvjson.DecodeScriptCmd{HexScript: "00"},
		},
		{
			name: "getaddednodeinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getaddednodeinfo", true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetAddedNodeInfoCmd(true, nil)
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getaddednodeinfo","params":[true],"id":1}`,
			unmarshalled: &bsvjson.GetAddedNodeInfoCmd{DNS: true, Node: nil},
		},
		{
			name: "getaddednodeinfo optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getaddednodeinfo", true, "127.0.0.1")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetAddedNodeInfoCmd(true, bsvjson.String("127.0.0.1"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getaddednodeinfo","params":[true,"127.0.0.1"],"id":1}`,
			unmarshalled: &bsvjson.GetAddedNodeInfoCmd{
				DNS:  true,
				Node: bsvjson.String("127.0.0.1"),
			},
		},
		{
			name: "getbestblockhash",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getbestblockhash")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBestBlockHashCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getbestblockhash","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetBestBlockHashCmd{},
		},
		{
			name: "getblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblock", "123", 0)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockCmd("123", bsvjson.Uint32(0))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblock","params":["123",0],"id":1}`,
			unmarshalled: &bsvjson.GetBlockCmd{
				Hash:      "123",
				Verbosity: bsvjson.Uint32(0),
			},
		},
		{
			name: "getblock required optional1",
			newCmd: func() (interface{}, error) {
				// Intentionally use a source param that is
				// more pointers than the destination to
				// exercise that path.
				verbosityPtr := bsvjson.Uint32(1)
				return bsvjson.NewCmd("getblock", "123", &verbosityPtr)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockCmd("123", bsvjson.Uint32(1))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblock","params":["123",1],"id":1}`,
			unmarshalled: &bsvjson.GetBlockCmd{
				Hash:      "123",
				Verbosity: bsvjson.Uint32(1),
			},
		},
		{
			name: "getblock required optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblock", "123", 2)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockCmd("123", bsvjson.Uint32(2))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblock","params":["123",2],"id":1}`,
			unmarshalled: &bsvjson.GetBlockCmd{
				Hash:      "123",
				Verbosity: bsvjson.Uint32(2),
			},
		},
		{
			name: "getblock; default verbose level must be 1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblock", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockCmd("123", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblock","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.GetBlockCmd{
				Hash:      "123",
				Verbosity: bsvjson.Uint32(1),
			},
		},
		{
			name: "getblockchaininfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblockchaininfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockChainInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getblockchaininfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetBlockChainInfoCmd{},
		},
		{
			name: "getblockcount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblockcount")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockCountCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getblockcount","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetBlockCountCmd{},
		},
		{
			name: "getblockhash",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblockhash", 123)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockHashCmd(123)
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getblockhash","params":[123],"id":1}`,
			unmarshalled: &bsvjson.GetBlockHashCmd{Index: 123},
		},
		{
			name: "getblockheader",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblockheader", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockHeaderCmd("123", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblockheader","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.GetBlockHeaderCmd{
				Hash:    "123",
				Verbose: bsvjson.Bool(true),
			},
		},
		{
			name: "getblocktemplate",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblocktemplate")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBlockTemplateCmd(nil)
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getblocktemplate","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetBlockTemplateCmd{Request: nil},
		},
		{
			name: "getblocktemplate optional - template request",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblocktemplate", `{"mode":"template","capabilities":["longpoll","coinbasetxn"]}`)
			},
			staticCmd: func() interface{} {
				template := bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
				}
				return bsvjson.NewGetBlockTemplateCmd(&template)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblocktemplate","params":[{"mode":"template","capabilities":["longpoll","coinbasetxn"]}],"id":1}`,
			unmarshalled: &bsvjson.GetBlockTemplateCmd{
				Request: &bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
				},
			},
		},
		{
			name: "getblocktemplate optional - template request with tweaks",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblocktemplate", `{"mode":"template","capabilities":["longpoll","coinbasetxn"],"sigoplimit":500,"sizelimit":100000000,"maxversion":2}`)
			},
			staticCmd: func() interface{} {
				template := bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
					SigOpLimit:   500,
					SizeLimit:    100000000,
					MaxVersion:   2,
				}
				return bsvjson.NewGetBlockTemplateCmd(&template)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblocktemplate","params":[{"mode":"template","capabilities":["longpoll","coinbasetxn"],"sigoplimit":500,"sizelimit":100000000,"maxversion":2}],"id":1}`,
			unmarshalled: &bsvjson.GetBlockTemplateCmd{
				Request: &bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
					SigOpLimit:   int64(500),
					SizeLimit:    int64(100000000),
					MaxVersion:   2,
				},
			},
		},
		{
			name: "getblocktemplate optional - template request with tweaks 2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getblocktemplate", `{"mode":"template","capabilities":["longpoll","coinbasetxn"],"sigoplimit":true,"sizelimit":100000000,"maxversion":2}`)
			},
			staticCmd: func() interface{} {
				template := bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
					SigOpLimit:   true,
					SizeLimit:    100000000,
					MaxVersion:   2,
				}
				return bsvjson.NewGetBlockTemplateCmd(&template)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getblocktemplate","params":[{"mode":"template","capabilities":["longpoll","coinbasetxn"],"sigoplimit":true,"sizelimit":100000000,"maxversion":2}],"id":1}`,
			unmarshalled: &bsvjson.GetBlockTemplateCmd{
				Request: &bsvjson.TemplateRequest{
					Mode:         "template",
					Capabilities: []string{"longpoll", "coinbasetxn"},
					SigOpLimit:   true,
					SizeLimit:    int64(100000000),
					MaxVersion:   2,
				},
			},
		},
		{
			name: "getcfilter",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getcfilter", "123",
					wire.GCSFilterRegular)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetCFilterCmd("123",
					wire.GCSFilterRegular)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getcfilter","params":["123",0],"id":1}`,
			unmarshalled: &bsvjson.GetCFilterCmd{
				Hash:       "123",
				FilterType: wire.GCSFilterRegular,
			},
		},
		{
			name: "getcfilterheader",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getcfilterheader", "123",
					wire.GCSFilterRegular)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetCFilterHeaderCmd("123",
					wire.GCSFilterRegular)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getcfilterheader","params":["123",0],"id":1}`,
			unmarshalled: &bsvjson.GetCFilterHeaderCmd{
				Hash:       "123",
				FilterType: wire.GCSFilterRegular,
			},
		},
		{
			name: "getchaintips",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getchaintips")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetChainTipsCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getchaintips","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetChainTipsCmd{},
		},
		{
			name: "getconnectioncount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getconnectioncount")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetConnectionCountCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getconnectioncount","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetConnectionCountCmd{},
		},
		{
			name: "getdifficulty",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getdifficulty")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetDifficultyCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getdifficulty","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetDifficultyCmd{},
		},
		{
			name: "getgenerate",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getgenerate")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetGenerateCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getgenerate","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetGenerateCmd{},
		},
		{
			name: "gethashespersec",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gethashespersec")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetHashesPerSecCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"gethashespersec","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetHashesPerSecCmd{},
		},
		{
			name: "getinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetInfoCmd{},
		},
		{
			name: "getmempoolentry",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getmempoolentry", "txhash")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetMempoolEntryCmd("txhash")
			},
			marshalled: `{"jsonrpc":"1.0","method":"getmempoolentry","params":["txhash"],"id":1}`,
			unmarshalled: &bsvjson.GetMempoolEntryCmd{
				TxID: "txhash",
			},
		},
		{
			name: "getmempoolinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getmempoolinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetMempoolInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getmempoolinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetMempoolInfoCmd{},
		},
		{
			name: "getmininginfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getmininginfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetMiningInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getmininginfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetMiningInfoCmd{},
		},
		{
			name: "getnetworkinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnetworkinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNetworkInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getnetworkinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetNetworkInfoCmd{},
		},
		{
			name: "getnettotals",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnettotals")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNetTotalsCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getnettotals","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetNetTotalsCmd{},
		},
		{
			name: "getnetworkhashps",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnetworkhashps")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNetworkHashPSCmd(nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getnetworkhashps","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetNetworkHashPSCmd{
				Blocks: bsvjson.Int(120),
				Height: bsvjson.Int(-1),
			},
		},
		{
			name: "getnetworkhashps optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnetworkhashps", 200)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNetworkHashPSCmd(bsvjson.Int(200), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getnetworkhashps","params":[200],"id":1}`,
			unmarshalled: &bsvjson.GetNetworkHashPSCmd{
				Blocks: bsvjson.Int(200),
				Height: bsvjson.Int(-1),
			},
		},
		{
			name: "getnetworkhashps optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnetworkhashps", 200, 123)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNetworkHashPSCmd(bsvjson.Int(200), bsvjson.Int(123))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getnetworkhashps","params":[200,123],"id":1}`,
			unmarshalled: &bsvjson.GetNetworkHashPSCmd{
				Blocks: bsvjson.Int(200),
				Height: bsvjson.Int(123),
			},
		},
		{
			name: "getpeerinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getpeerinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetPeerInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getpeerinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetPeerInfoCmd{},
		},
		{
			name: "getrawmempool",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawmempool")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawMempoolCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawmempool","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetRawMempoolCmd{
				Verbose: bsvjson.Bool(false),
			},
		},
		{
			name: "getrawmempool optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawmempool", false)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawMempoolCmd(bsvjson.Bool(false))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawmempool","params":[false],"id":1}`,
			unmarshalled: &bsvjson.GetRawMempoolCmd{
				Verbose: bsvjson.Bool(false),
			},
		},
		{
			name: "getrawtransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawtransaction", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawTransactionCmd("123", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawtransaction","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.GetRawTransactionCmd{
				Txid:    "123",
				Verbose: bsvjson.Int(0),
			},
		},
		{
			name: "getrawtransaction optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawtransaction", "123", 1)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawTransactionCmd("123", bsvjson.Int(1))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawtransaction","params":["123",1],"id":1}`,
			unmarshalled: &bsvjson.GetRawTransactionCmd{
				Txid:    "123",
				Verbose: bsvjson.Int(1),
			},
		},
		{
			name: "gettxout",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettxout", "123", 1)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTxOutCmd("123", 1, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettxout","params":["123",1],"id":1}`,
			unmarshalled: &bsvjson.GetTxOutCmd{
				Txid:           "123",
				Vout:           1,
				IncludeMempool: bsvjson.Bool(true),
			},
		},
		{
			name: "gettxout optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettxout", "123", 1, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTxOutCmd("123", 1, bsvjson.Bool(true))
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettxout","params":["123",1,true],"id":1}`,
			unmarshalled: &bsvjson.GetTxOutCmd{
				Txid:           "123",
				Vout:           1,
				IncludeMempool: bsvjson.Bool(true),
			},
		},
		{
			name: "gettxoutproof",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettxoutproof", []string{"123", "456"})
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTxOutProofCmd([]string{"123", "456"}, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettxoutproof","params":[["123","456"]],"id":1}`,
			unmarshalled: &bsvjson.GetTxOutProofCmd{
				TxIDs: []string{"123", "456"},
			},
		},
		{
			name: "gettxoutproof optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettxoutproof", []string{"123", "456"},
					bsvjson.String("000000000000034a7dedef4a161fa058a2d67a173a90155f3a2fe6fc132e0ebf"))
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTxOutProofCmd([]string{"123", "456"},
					bsvjson.String("000000000000034a7dedef4a161fa058a2d67a173a90155f3a2fe6fc132e0ebf"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettxoutproof","params":[["123","456"],` +
				`"000000000000034a7dedef4a161fa058a2d67a173a90155f3a2fe6fc132e0ebf"],"id":1}`,
			unmarshalled: &bsvjson.GetTxOutProofCmd{
				TxIDs:     []string{"123", "456"},
				BlockHash: bsvjson.String("000000000000034a7dedef4a161fa058a2d67a173a90155f3a2fe6fc132e0ebf"),
			},
		},
		{
			name: "gettxoutsetinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettxoutsetinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTxOutSetInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"gettxoutsetinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetTxOutSetInfoCmd{},
		},
		{
			name: "getwork",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getwork")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetWorkCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getwork","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetWorkCmd{
				Data: nil,
			},
		},
		{
			name: "getwork optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getwork", "00112233")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetWorkCmd(bsvjson.String("00112233"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getwork","params":["00112233"],"id":1}`,
			unmarshalled: &bsvjson.GetWorkCmd{
				Data: bsvjson.String("00112233"),
			},
		},
		{
			name: "help",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("help")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewHelpCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"help","params":[],"id":1}`,
			unmarshalled: &bsvjson.HelpCmd{
				Command: nil,
			},
		},
		{
			name: "help optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("help", "getblock")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewHelpCmd(bsvjson.String("getblock"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"help","params":["getblock"],"id":1}`,
			unmarshalled: &bsvjson.HelpCmd{
				Command: bsvjson.String("getblock"),
			},
		},
		{
			name: "invalidateblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("invalidateblock", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewInvalidateBlockCmd("123")
			},
			marshalled: `{"jsonrpc":"1.0","method":"invalidateblock","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.InvalidateBlockCmd{
				BlockHash: "123",
			},
		},
		{
			name: "ping",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("ping")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewPingCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"ping","params":[],"id":1}`,
			unmarshalled: &bsvjson.PingCmd{},
		},
		{
			name: "preciousblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("preciousblock", "0123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewPreciousBlockCmd("0123")
			},
			marshalled: `{"jsonrpc":"1.0","method":"preciousblock","params":["0123"],"id":1}`,
			unmarshalled: &bsvjson.PreciousBlockCmd{
				BlockHash: "0123",
			},
		},
		{
			name: "reconsiderblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("reconsiderblock", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewReconsiderBlockCmd("123")
			},
			marshalled: `{"jsonrpc":"1.0","method":"reconsiderblock","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.ReconsiderBlockCmd{
				BlockHash: "123",
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address", nil, nil, nil, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address"],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(1),
				Skip:        bsvjson.Int(0),
				Count:       bsvjson.Int(100),
				VinExtra:    bsvjson.Int(0),
				Reverse:     bsvjson.Bool(false),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), nil, nil, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(0),
				Count:       bsvjson.Int(100),
				VinExtra:    bsvjson.Int(0),
				Reverse:     bsvjson.Bool(false),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0, 5)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), bsvjson.Int(5), nil, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0,5],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(5),
				Count:       bsvjson.Int(100),
				VinExtra:    bsvjson.Int(0),
				Reverse:     bsvjson.Bool(false),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0, 5, 10)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), bsvjson.Int(5), bsvjson.Int(10), nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0,5,10],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(5),
				Count:       bsvjson.Int(10),
				VinExtra:    bsvjson.Int(0),
				Reverse:     bsvjson.Bool(false),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0, 5, 10, 1)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), bsvjson.Int(5), bsvjson.Int(10), bsvjson.Int(1), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0,5,10,1],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(5),
				Count:       bsvjson.Int(10),
				VinExtra:    bsvjson.Int(1),
				Reverse:     bsvjson.Bool(false),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0, 5, 10, 1, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), bsvjson.Int(5), bsvjson.Int(10), bsvjson.Int(1), bsvjson.Bool(true), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0,5,10,1,true],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(5),
				Count:       bsvjson.Int(10),
				VinExtra:    bsvjson.Int(1),
				Reverse:     bsvjson.Bool(true),
				FilterAddrs: nil,
			},
		},
		{
			name: "searchrawtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("searchrawtransactions", "1Address", 0, 5, 10, 1, true, []string{"1Address"})
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSearchRawTransactionsCmd("1Address",
					bsvjson.Int(0), bsvjson.Int(5), bsvjson.Int(10), bsvjson.Int(1), bsvjson.Bool(true), &[]string{"1Address"})
			},
			marshalled: `{"jsonrpc":"1.0","method":"searchrawtransactions","params":["1Address",0,5,10,1,true,["1Address"]],"id":1}`,
			unmarshalled: &bsvjson.SearchRawTransactionsCmd{
				Address:     "1Address",
				Verbose:     bsvjson.Int(0),
				Skip:        bsvjson.Int(5),
				Count:       bsvjson.Int(10),
				VinExtra:    bsvjson.Int(1),
				Reverse:     bsvjson.Bool(true),
				FilterAddrs: &[]string{"1Address"},
			},
		},
		{
			name: "sendrawtransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendrawtransaction", "1122")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendRawTransactionCmd("1122", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendrawtransaction","params":["1122"],"id":1}`,
			unmarshalled: &bsvjson.SendRawTransactionCmd{
				HexTx:         "1122",
				AllowHighFees: bsvjson.Bool(false),
			},
		},
		{
			name: "sendrawtransaction optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendrawtransaction", "1122", false)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendRawTransactionCmd("1122", bsvjson.Bool(false))
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendrawtransaction","params":["1122",false],"id":1}`,
			unmarshalled: &bsvjson.SendRawTransactionCmd{
				HexTx:         "1122",
				AllowHighFees: bsvjson.Bool(false),
			},
		},
		{
			name: "setgenerate",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("setgenerate", true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSetGenerateCmd(true, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"setgenerate","params":[true],"id":1}`,
			unmarshalled: &bsvjson.SetGenerateCmd{
				Generate:     true,
				GenProcLimit: bsvjson.Int(-1),
			},
		},
		{
			name: "setgenerate optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("setgenerate", true, 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSetGenerateCmd(true, bsvjson.Int(6))
			},
			marshalled: `{"jsonrpc":"1.0","method":"setgenerate","params":[true,6],"id":1}`,
			unmarshalled: &bsvjson.SetGenerateCmd{
				Generate:     true,
				GenProcLimit: bsvjson.Int(6),
			},
		},
		{
			name: "stop",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("stop")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewStopCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"stop","params":[],"id":1}`,
			unmarshalled: &bsvjson.StopCmd{},
		},
		{
			name: "submitblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("submitblock", "112233")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSubmitBlockCmd("112233", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"submitblock","params":["112233"],"id":1}`,
			unmarshalled: &bsvjson.SubmitBlockCmd{
				HexBlock: "112233",
				Options:  nil,
			},
		},
		{
			name: "submitblock optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("submitblock", "112233", `{"workid":"12345"}`)
			},
			staticCmd: func() interface{} {
				options := bsvjson.SubmitBlockOptions{
					WorkID: "12345",
				}
				return bsvjson.NewSubmitBlockCmd("112233", &options)
			},
			marshalled: `{"jsonrpc":"1.0","method":"submitblock","params":["112233",{"workid":"12345"}],"id":1}`,
			unmarshalled: &bsvjson.SubmitBlockCmd{
				HexBlock: "112233",
				Options: &bsvjson.SubmitBlockOptions{
					WorkID: "12345",
				},
			},
		},
		{
			name: "uptime",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("uptime")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewUptimeCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"uptime","params":[],"id":1}`,
			unmarshalled: &bsvjson.UptimeCmd{},
		},
		{
			name: "validateaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("validateaddress", "1Address")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewValidateAddressCmd("1Address")
			},
			marshalled: `{"jsonrpc":"1.0","method":"validateaddress","params":["1Address"],"id":1}`,
			unmarshalled: &bsvjson.ValidateAddressCmd{
				Address: "1Address",
			},
		},
		{
			name: "verifychain",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("verifychain")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewVerifyChainCmd(nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"verifychain","params":[],"id":1}`,
			unmarshalled: &bsvjson.VerifyChainCmd{
				CheckLevel: bsvjson.Int32(3),
				CheckDepth: bsvjson.Int32(288),
			},
		},
		{
			name: "verifychain optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("verifychain", 2)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewVerifyChainCmd(bsvjson.Int32(2), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"verifychain","params":[2],"id":1}`,
			unmarshalled: &bsvjson.VerifyChainCmd{
				CheckLevel: bsvjson.Int32(2),
				CheckDepth: bsvjson.Int32(288),
			},
		},
		{
			name: "verifychain optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("verifychain", 2, 500)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewVerifyChainCmd(bsvjson.Int32(2), bsvjson.Int32(500))
			},
			marshalled: `{"jsonrpc":"1.0","method":"verifychain","params":[2,500],"id":1}`,
			unmarshalled: &bsvjson.VerifyChainCmd{
				CheckLevel: bsvjson.Int32(2),
				CheckDepth: bsvjson.Int32(500),
			},
		},
		{
			name: "verifymessage",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("verifymessage", "1Address", "301234", "test")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewVerifyMessageCmd("1Address", "301234", "test")
			},
			marshalled: `{"jsonrpc":"1.0","method":"verifymessage","params":["1Address","301234","test"],"id":1}`,
			unmarshalled: &bsvjson.VerifyMessageCmd{
				Address:   "1Address",
				Signature: "301234",
				Message:   "test",
			},
		},
		{
			name: "verifytxoutproof",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("verifytxoutproof", "test")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewVerifyTxOutProofCmd("test")
			},
			marshalled: `{"jsonrpc":"1.0","method":"verifytxoutproof","params":["test"],"id":1}`,
			unmarshalled: &bsvjson.VerifyTxOutProofCmd{
				Proof: "test",
			},
		},
	}

	t.Logf("Running %d tests", len(tests))

	for i, test := range tests {
		// Marshal the command as created by the new static command
		// creation function.
		marshalled, err := bsvjson.MarshalCmd(testID, test.staticCmd())
		if err != nil {
			t.Errorf("MarshalCmd #%d (%s) unexpected error: %v", i,
				test.name, err)
			continue
		}

		if !bytes.Equal(marshalled, []byte(test.marshalled)) {
			t.Errorf("Test #%d (%s) unexpected marshalled data - "+
				"got %s, want %s", i, test.name, marshalled,
				test.marshalled)
			t.Errorf("\n%s\n%s", marshalled, test.marshalled)

			continue
		}

		// Ensure the command is created without error via the generic
		// new command creation function.
		cmd, err := test.newCmd()
		if err != nil {
			t.Errorf("Test #%d (%s) unexpected NewCmd error: %v ",
				i, test.name, err)
		}

		// Marshal the command as created by the generic new command
		// creation function.
		marshalled, err = bsvjson.MarshalCmd(testID, cmd)
		if err != nil {
			t.Errorf("MarshalCmd #%d (%s) unexpected error: %v", i,
				test.name, err)
			continue
		}

		if !bytes.Equal(marshalled, []byte(test.marshalled)) {
			t.Errorf("Test #%d (%s) unexpected marshalled data - "+
				"got %s, want %s", i, test.name, marshalled,
				test.marshalled)

			continue
		}

		var request bsvjson.Request
		if err := json.Unmarshal(marshalled, &request); err != nil {
			t.Errorf("Test #%d (%s) unexpected error while "+
				"unmarshalling JSON-RPC request: %v", i,
				test.name, err)

			continue
		}

		cmd, err = bsvjson.UnmarshalCmd(&request)
		if err != nil {
			t.Errorf("UnmarshalCmd #%d (%s) unexpected error: %v", i,
				test.name, err)
			continue
		}

		if !reflect.DeepEqual(cmd, test.unmarshalled) {
			t.Errorf("Test #%d (%s) unexpected unmarshalled command "+
				"- got %s, want %s", i, test.name,
				fmt.Sprintf("(%T) %+[1]v", cmd),
				fmt.Sprintf("(%T) %+[1]v\n", test.unmarshalled))

			continue
		}
	}
}

// TestChainSvrCmdErrors ensures any errors that occur in the command during
// custom mashal and unmarshal are as expected.
func TestChainSvrCmdErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		result     interface{}
		marshalled string
		err        error
	}{
		{
			name:       "template request with invalid type",
			result:     &bsvjson.TemplateRequest{},
			marshalled: `{"mode":1}`,
			err:        &json.UnmarshalTypeError{},
		},
		{
			name:       "invalid template request sigoplimit field",
			result:     &bsvjson.TemplateRequest{},
			marshalled: `{"sigoplimit":"invalid"}`,
			err:        bsvjson.Error{ErrorCode: bsvjson.ErrInvalidType},
		},
		{
			name:       "invalid template request sizelimit field",
			result:     &bsvjson.TemplateRequest{},
			marshalled: `{"sizelimit":"invalid"}`,
			err:        bsvjson.Error{ErrorCode: bsvjson.ErrInvalidType},
		},
	}

	t.Logf("Running %d tests", len(tests))

	for i, test := range tests {
		err := json.Unmarshal([]byte(test.marshalled), &test.result)
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("Test #%d (%s) wrong error - got %T (%v), "+
				"want %T", i, test.name, err, err, test.err)
			continue
		}

		if terr, ok := test.err.(bsvjson.Error); ok {
			gotErrorCode := err.(bsvjson.Error).ErrorCode
			if gotErrorCode != terr.ErrorCode {
				t.Errorf("Test #%d (%s) mismatched error code "+
					"- got %v (%v), want %v", i, test.name,
					gotErrorCode, terr, terr.ErrorCode)

				continue
			}
		}
	}
}
