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

	"github.com/bitcoin-sv/teranode/services/rpc/bsvjson"
)

// TestWalletSvrCmds tests all of the wallet server commands marshal and
// unmarshal into valid results include handling of optional fields being
// omitted in the marshalled command, while optional fields with defaults have
// the default assigned on unmarshalled commands.
func TestWalletSvrCmds(t *testing.T) {
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
			name: "addmultisigaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("addmultisigaddress", 2, []string{"031234", "035678"})
			},
			staticCmd: func() interface{} {
				keys := []string{"031234", "035678"}
				return bsvjson.NewAddMultisigAddressCmd(2, keys, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"addmultisigaddress","params":[2,["031234","035678"]],"id":1}`,
			unmarshalled: &bsvjson.AddMultisigAddressCmd{
				NRequired: 2,
				Keys:      []string{"031234", "035678"},
				Account:   nil,
			},
		},
		{
			name: "addmultisigaddress optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("addmultisigaddress", 2, []string{"031234", "035678"}, "test")
			},
			staticCmd: func() interface{} {
				keys := []string{"031234", "035678"}
				return bsvjson.NewAddMultisigAddressCmd(2, keys, bsvjson.String("test"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"addmultisigaddress","params":[2,["031234","035678"],"test"],"id":1}`,
			unmarshalled: &bsvjson.AddMultisigAddressCmd{
				NRequired: 2,
				Keys:      []string{"031234", "035678"},
				Account:   bsvjson.String("test"),
			},
		},
		{
			name: "createmultisig",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("createmultisig", 2, []string{"031234", "035678"})
			},
			staticCmd: func() interface{} {
				keys := []string{"031234", "035678"}
				return bsvjson.NewCreateMultisigCmd(2, keys)
			},
			marshalled: `{"jsonrpc":"1.0","method":"createmultisig","params":[2,["031234","035678"]],"id":1}`,
			unmarshalled: &bsvjson.CreateMultisigCmd{
				NRequired: 2,
				Keys:      []string{"031234", "035678"},
			},
		},
		{
			name: "dumpprivkey",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("dumpprivkey", "1Address")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewDumpPrivKeyCmd("1Address")
			},
			marshalled: `{"jsonrpc":"1.0","method":"dumpprivkey","params":["1Address"],"id":1}`,
			unmarshalled: &bsvjson.DumpPrivKeyCmd{
				Address: "1Address",
			},
		},
		{
			name: "encryptwallet",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("encryptwallet", "pass")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewEncryptWalletCmd("pass")
			},
			marshalled: `{"jsonrpc":"1.0","method":"encryptwallet","params":["pass"],"id":1}`,
			unmarshalled: &bsvjson.EncryptWalletCmd{
				Passphrase: "pass",
			},
		},
		{
			name: "estimatefee",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("estimatefee", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewEstimateFeeCmd(6)
			},
			marshalled: `{"jsonrpc":"1.0","method":"estimatefee","params":[6],"id":1}`,
			unmarshalled: &bsvjson.EstimateFeeCmd{
				NumBlocks: 6,
			},
		},
		{
			name: "estimatepriority",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("estimatepriority", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewEstimatePriorityCmd(6)
			},
			marshalled: `{"jsonrpc":"1.0","method":"estimatepriority","params":[6],"id":1}`,
			unmarshalled: &bsvjson.EstimatePriorityCmd{
				NumBlocks: 6,
			},
		},
		{
			name: "getaccount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getaccount", "1Address")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetAccountCmd("1Address")
			},
			marshalled: `{"jsonrpc":"1.0","method":"getaccount","params":["1Address"],"id":1}`,
			unmarshalled: &bsvjson.GetAccountCmd{
				Address: "1Address",
			},
		},
		{
			name: "getaccountaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getaccountaddress", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetAccountAddressCmd("acct")
			},
			marshalled: `{"jsonrpc":"1.0","method":"getaccountaddress","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetAccountAddressCmd{
				Account: "acct",
			},
		},
		{
			name: "getaddressesbyaccount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getaddressesbyaccount", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetAddressesByAccountCmd("acct")
			},
			marshalled: `{"jsonrpc":"1.0","method":"getaddressesbyaccount","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetAddressesByAccountCmd{
				Account: "acct",
			},
		},
		{
			name: "getbalance",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getbalance")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBalanceCmd(nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getbalance","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetBalanceCmd{
				Account: nil,
				MinConf: bsvjson.Int(1),
			},
		},
		{
			name: "getbalance optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getbalance", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBalanceCmd(bsvjson.String("acct"), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getbalance","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetBalanceCmd{
				Account: bsvjson.String("acct"),
				MinConf: bsvjson.Int(1),
			},
		},
		{
			name: "getbalance optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getbalance", "acct", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetBalanceCmd(bsvjson.String("acct"), bsvjson.Int(6))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getbalance","params":["acct",6],"id":1}`,
			unmarshalled: &bsvjson.GetBalanceCmd{
				Account: bsvjson.String("acct"),
				MinConf: bsvjson.Int(6),
			},
		},
		{
			name: "getnewaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnewaddress")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNewAddressCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getnewaddress","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetNewAddressCmd{
				Account: nil,
			},
		},
		{
			name: "getnewaddress optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getnewaddress", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetNewAddressCmd(bsvjson.String("acct"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getnewaddress","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetNewAddressCmd{
				Account: bsvjson.String("acct"),
			},
		},
		{
			name: "getrawchangeaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawchangeaddress")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawChangeAddressCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawchangeaddress","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetRawChangeAddressCmd{
				Account: nil,
			},
		},
		{
			name: "getrawchangeaddress optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getrawchangeaddress", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetRawChangeAddressCmd(bsvjson.String("acct"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getrawchangeaddress","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetRawChangeAddressCmd{
				Account: bsvjson.String("acct"),
			},
		},
		{
			name: "getreceivedbyaccount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getreceivedbyaccount", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetReceivedByAccountCmd("acct", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getreceivedbyaccount","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.GetReceivedByAccountCmd{
				Account: "acct",
				MinConf: bsvjson.Int(1),
			},
		},
		{
			name: "getreceivedbyaccount optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getreceivedbyaccount", "acct", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetReceivedByAccountCmd("acct", bsvjson.Int(6))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getreceivedbyaccount","params":["acct",6],"id":1}`,
			unmarshalled: &bsvjson.GetReceivedByAccountCmd{
				Account: "acct",
				MinConf: bsvjson.Int(6),
			},
		},
		{
			name: "getreceivedbyaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getreceivedbyaddress", "1Address")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetReceivedByAddressCmd("1Address", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"getreceivedbyaddress","params":["1Address"],"id":1}`,
			unmarshalled: &bsvjson.GetReceivedByAddressCmd{
				Address: "1Address",
				MinConf: bsvjson.Int(1),
			},
		},
		{
			name: "getreceivedbyaddress optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getreceivedbyaddress", "1Address", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetReceivedByAddressCmd("1Address", bsvjson.Int(6))
			},
			marshalled: `{"jsonrpc":"1.0","method":"getreceivedbyaddress","params":["1Address",6],"id":1}`,
			unmarshalled: &bsvjson.GetReceivedByAddressCmd{
				Address: "1Address",
				MinConf: bsvjson.Int(6),
			},
		},
		{
			name: "gettransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettransaction", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTransactionCmd("123", nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettransaction","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.GetTransactionCmd{
				Txid:             "123",
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "gettransaction optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("gettransaction", "123", true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetTransactionCmd("123", bsvjson.Bool(true))
			},
			marshalled: `{"jsonrpc":"1.0","method":"gettransaction","params":["123",true],"id":1}`,
			unmarshalled: &bsvjson.GetTransactionCmd{
				Txid:             "123",
				IncludeWatchOnly: bsvjson.Bool(true),
			},
		},
		{
			name: "getwalletinfo",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("getwalletinfo")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewGetWalletInfoCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"getwalletinfo","params":[],"id":1}`,
			unmarshalled: &bsvjson.GetWalletInfoCmd{},
		},
		{
			name: "importprivkey",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("importprivkey", "abc")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewImportPrivKeyCmd("abc", nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"importprivkey","params":["abc"],"id":1}`,
			unmarshalled: &bsvjson.ImportPrivKeyCmd{
				PrivKey: "abc",
				Label:   nil,
				Rescan:  bsvjson.Bool(true),
			},
		},
		{
			name: "importprivkey optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("importprivkey", "abc", "label")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewImportPrivKeyCmd("abc", bsvjson.String("label"), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"importprivkey","params":["abc","label"],"id":1}`,
			unmarshalled: &bsvjson.ImportPrivKeyCmd{
				PrivKey: "abc",
				Label:   bsvjson.String("label"),
				Rescan:  bsvjson.Bool(true),
			},
		},
		{
			name: "importprivkey optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("importprivkey", "abc", "label", false)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewImportPrivKeyCmd("abc", bsvjson.String("label"), bsvjson.Bool(false))
			},
			marshalled: `{"jsonrpc":"1.0","method":"importprivkey","params":["abc","label",false],"id":1}`,
			unmarshalled: &bsvjson.ImportPrivKeyCmd{
				PrivKey: "abc",
				Label:   bsvjson.String("label"),
				Rescan:  bsvjson.Bool(false),
			},
		},
		{
			name: "keypoolrefill",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("keypoolrefill")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewKeyPoolRefillCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"keypoolrefill","params":[],"id":1}`,
			unmarshalled: &bsvjson.KeyPoolRefillCmd{
				NewSize: bsvjson.Uint(100),
			},
		},
		{
			name: "keypoolrefill optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("keypoolrefill", 200)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewKeyPoolRefillCmd(bsvjson.Uint(200))
			},
			marshalled: `{"jsonrpc":"1.0","method":"keypoolrefill","params":[200],"id":1}`,
			unmarshalled: &bsvjson.KeyPoolRefillCmd{
				NewSize: bsvjson.Uint(200),
			},
		},
		{
			name: "listaccounts",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listaccounts")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListAccountsCmd(nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listaccounts","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListAccountsCmd{
				MinConf: bsvjson.Int(1),
			},
		},
		{
			name: "listaccounts optional",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listaccounts", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListAccountsCmd(bsvjson.Int(6))
			},
			marshalled: `{"jsonrpc":"1.0","method":"listaccounts","params":[6],"id":1}`,
			unmarshalled: &bsvjson.ListAccountsCmd{
				MinConf: bsvjson.Int(6),
			},
		},
		{
			name: "listaddressgroupings",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listaddressgroupings")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListAddressGroupingsCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"listaddressgroupings","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListAddressGroupingsCmd{},
		},
		{
			name: "listlockunspent",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listlockunspent")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListLockUnspentCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"listlockunspent","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListLockUnspentCmd{},
		},
		{
			name: "listreceivedbyaccount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaccount")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAccountCmd(nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaccount","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAccountCmd{
				MinConf:          bsvjson.Int(1),
				IncludeEmpty:     bsvjson.Bool(false),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaccount optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaccount", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAccountCmd(bsvjson.Int(6), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaccount","params":[6],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAccountCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(false),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaccount optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaccount", 6, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAccountCmd(bsvjson.Int(6), bsvjson.Bool(true), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaccount","params":[6,true],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAccountCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(true),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaccount optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaccount", 6, true, false)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAccountCmd(bsvjson.Int(6), bsvjson.Bool(true), bsvjson.Bool(false))
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaccount","params":[6,true,false],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAccountCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(true),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaddress")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAddressCmd(nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaddress","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAddressCmd{
				MinConf:          bsvjson.Int(1),
				IncludeEmpty:     bsvjson.Bool(false),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaddress optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaddress", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAddressCmd(bsvjson.Int(6), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaddress","params":[6],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAddressCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(false),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaddress optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaddress", 6, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAddressCmd(bsvjson.Int(6), bsvjson.Bool(true), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaddress","params":[6,true],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAddressCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(true),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listreceivedbyaddress optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listreceivedbyaddress", 6, true, false)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListReceivedByAddressCmd(bsvjson.Int(6), bsvjson.Bool(true), bsvjson.Bool(false))
			},
			marshalled: `{"jsonrpc":"1.0","method":"listreceivedbyaddress","params":[6,true,false],"id":1}`,
			unmarshalled: &bsvjson.ListReceivedByAddressCmd{
				MinConf:          bsvjson.Int(6),
				IncludeEmpty:     bsvjson.Bool(true),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listsinceblock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listsinceblock")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListSinceBlockCmd(nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listsinceblock","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListSinceBlockCmd{
				BlockHash:           nil,
				TargetConfirmations: bsvjson.Int(1),
				IncludeWatchOnly:    bsvjson.Bool(false),
			},
		},
		{
			name: "listsinceblock optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listsinceblock", "123")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListSinceBlockCmd(bsvjson.String("123"), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listsinceblock","params":["123"],"id":1}`,
			unmarshalled: &bsvjson.ListSinceBlockCmd{
				BlockHash:           bsvjson.String("123"),
				TargetConfirmations: bsvjson.Int(1),
				IncludeWatchOnly:    bsvjson.Bool(false),
			},
		},
		{
			name: "listsinceblock optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listsinceblock", "123", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListSinceBlockCmd(bsvjson.String("123"), bsvjson.Int(6), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listsinceblock","params":["123",6],"id":1}`,
			unmarshalled: &bsvjson.ListSinceBlockCmd{
				BlockHash:           bsvjson.String("123"),
				TargetConfirmations: bsvjson.Int(6),
				IncludeWatchOnly:    bsvjson.Bool(false),
			},
		},
		{
			name: "listsinceblock optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listsinceblock", "123", 6, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListSinceBlockCmd(bsvjson.String("123"), bsvjson.Int(6), bsvjson.Bool(true))
			},
			marshalled: `{"jsonrpc":"1.0","method":"listsinceblock","params":["123",6,true],"id":1}`,
			unmarshalled: &bsvjson.ListSinceBlockCmd{
				BlockHash:           bsvjson.String("123"),
				TargetConfirmations: bsvjson.Int(6),
				IncludeWatchOnly:    bsvjson.Bool(true),
			},
		},
		{
			name: "listtransactions",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listtransactions")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListTransactionsCmd(nil, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listtransactions","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListTransactionsCmd{
				Account:          nil,
				Count:            bsvjson.Int(10),
				From:             bsvjson.Int(0),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listtransactions optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listtransactions", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListTransactionsCmd(bsvjson.String("acct"), nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listtransactions","params":["acct"],"id":1}`,
			unmarshalled: &bsvjson.ListTransactionsCmd{
				Account:          bsvjson.String("acct"),
				Count:            bsvjson.Int(10),
				From:             bsvjson.Int(0),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listtransactions optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listtransactions", "acct", 20)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListTransactionsCmd(bsvjson.String("acct"), bsvjson.Int(20), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listtransactions","params":["acct",20],"id":1}`,
			unmarshalled: &bsvjson.ListTransactionsCmd{
				Account:          bsvjson.String("acct"),
				Count:            bsvjson.Int(20),
				From:             bsvjson.Int(0),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listtransactions optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listtransactions", "acct", 20, 1)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListTransactionsCmd(bsvjson.String("acct"), bsvjson.Int(20),
					bsvjson.Int(1), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listtransactions","params":["acct",20,1],"id":1}`,
			unmarshalled: &bsvjson.ListTransactionsCmd{
				Account:          bsvjson.String("acct"),
				Count:            bsvjson.Int(20),
				From:             bsvjson.Int(1),
				IncludeWatchOnly: bsvjson.Bool(false),
			},
		},
		{
			name: "listtransactions optional4",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listtransactions", "acct", 20, 1, true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListTransactionsCmd(bsvjson.String("acct"), bsvjson.Int(20),
					bsvjson.Int(1), bsvjson.Bool(true))
			},
			marshalled: `{"jsonrpc":"1.0","method":"listtransactions","params":["acct",20,1,true],"id":1}`,
			unmarshalled: &bsvjson.ListTransactionsCmd{
				Account:          bsvjson.String("acct"),
				Count:            bsvjson.Int(20),
				From:             bsvjson.Int(1),
				IncludeWatchOnly: bsvjson.Bool(true),
			},
		},
		{
			name: "listunspent",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listunspent")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListUnspentCmd(nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listunspent","params":[],"id":1}`,
			unmarshalled: &bsvjson.ListUnspentCmd{
				MinConf:   bsvjson.Int(1),
				MaxConf:   bsvjson.Int(9999999),
				Addresses: nil,
			},
		},
		{
			name: "listunspent optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listunspent", 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListUnspentCmd(bsvjson.Int(6), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listunspent","params":[6],"id":1}`,
			unmarshalled: &bsvjson.ListUnspentCmd{
				MinConf:   bsvjson.Int(6),
				MaxConf:   bsvjson.Int(9999999),
				Addresses: nil,
			},
		},
		{
			name: "listunspent optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listunspent", 6, 100)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListUnspentCmd(bsvjson.Int(6), bsvjson.Int(100), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"listunspent","params":[6,100],"id":1}`,
			unmarshalled: &bsvjson.ListUnspentCmd{
				MinConf:   bsvjson.Int(6),
				MaxConf:   bsvjson.Int(100),
				Addresses: nil,
			},
		},
		{
			name: "listunspent optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("listunspent", 6, 100, []string{"1Address", "1Address2"})
			},
			staticCmd: func() interface{} {
				return bsvjson.NewListUnspentCmd(bsvjson.Int(6), bsvjson.Int(100),
					&[]string{"1Address", "1Address2"})
			},
			marshalled: `{"jsonrpc":"1.0","method":"listunspent","params":[6,100,["1Address","1Address2"]],"id":1}`,
			unmarshalled: &bsvjson.ListUnspentCmd{
				MinConf:   bsvjson.Int(6),
				MaxConf:   bsvjson.Int(100),
				Addresses: &[]string{"1Address", "1Address2"},
			},
		},
		{
			name: "lockunspent",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("lockunspent", true, `[{"txid":"123","vout":1}]`)
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.TransactionInput{
					{Txid: "123", Vout: 1},
				}
				return bsvjson.NewLockUnspentCmd(true, txInputs)
			},
			marshalled: `{"jsonrpc":"1.0","method":"lockunspent","params":[true,[{"txid":"123","vout":1}]],"id":1}`,
			unmarshalled: &bsvjson.LockUnspentCmd{
				Unlock: true,
				Transactions: []bsvjson.TransactionInput{
					{Txid: "123", Vout: 1},
				},
			},
		},
		{
			name: "move",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("move", "from", "to", 0.5)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewMoveCmd("from", "to", 0.5, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"move","params":["from","to",0.5],"id":1}`,
			unmarshalled: &bsvjson.MoveCmd{
				FromAccount: "from",
				ToAccount:   "to",
				Amount:      0.5,
				MinConf:     bsvjson.Int(1),
				Comment:     nil,
			},
		},
		{
			name: "move optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("move", "from", "to", 0.5, 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewMoveCmd("from", "to", 0.5, bsvjson.Int(6), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"move","params":["from","to",0.5,6],"id":1}`,
			unmarshalled: &bsvjson.MoveCmd{
				FromAccount: "from",
				ToAccount:   "to",
				Amount:      0.5,
				MinConf:     bsvjson.Int(6),
				Comment:     nil,
			},
		},
		{
			name: "move optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("move", "from", "to", 0.5, 6, "comment")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewMoveCmd("from", "to", 0.5, bsvjson.Int(6), bsvjson.String("comment"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"move","params":["from","to",0.5,6,"comment"],"id":1}`,
			unmarshalled: &bsvjson.MoveCmd{
				FromAccount: "from",
				ToAccount:   "to",
				Amount:      0.5,
				MinConf:     bsvjson.Int(6),
				Comment:     bsvjson.String("comment"),
			},
		},
		{
			name: "sendfrom",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendfrom", "from", "1Address", 0.5)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendFromCmd("from", "1Address", 0.5, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendfrom","params":["from","1Address",0.5],"id":1}`,
			unmarshalled: &bsvjson.SendFromCmd{
				FromAccount: "from",
				ToAddress:   "1Address",
				Amount:      0.5,
				MinConf:     bsvjson.Int(1),
				Comment:     nil,
				CommentTo:   nil,
			},
		},
		{
			name: "sendfrom optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendfrom", "from", "1Address", 0.5, 6)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendFromCmd("from", "1Address", 0.5, bsvjson.Int(6), nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendfrom","params":["from","1Address",0.5,6],"id":1}`,
			unmarshalled: &bsvjson.SendFromCmd{
				FromAccount: "from",
				ToAddress:   "1Address",
				Amount:      0.5,
				MinConf:     bsvjson.Int(6),
				Comment:     nil,
				CommentTo:   nil,
			},
		},
		{
			name: "sendfrom optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendfrom", "from", "1Address", 0.5, 6, "comment")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendFromCmd("from", "1Address", 0.5, bsvjson.Int(6),
					bsvjson.String("comment"), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendfrom","params":["from","1Address",0.5,6,"comment"],"id":1}`,
			unmarshalled: &bsvjson.SendFromCmd{
				FromAccount: "from",
				ToAddress:   "1Address",
				Amount:      0.5,
				MinConf:     bsvjson.Int(6),
				Comment:     bsvjson.String("comment"),
				CommentTo:   nil,
			},
		},
		{
			name: "sendfrom optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendfrom", "from", "1Address", 0.5, 6, "comment", "commentto")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendFromCmd("from", "1Address", 0.5, bsvjson.Int(6),
					bsvjson.String("comment"), bsvjson.String("commentto"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendfrom","params":["from","1Address",0.5,6,"comment","commentto"],"id":1}`,
			unmarshalled: &bsvjson.SendFromCmd{
				FromAccount: "from",
				ToAddress:   "1Address",
				Amount:      0.5,
				MinConf:     bsvjson.Int(6),
				Comment:     bsvjson.String("comment"),
				CommentTo:   bsvjson.String("commentto"),
			},
		},
		{
			name: "sendmany",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendmany", "from", `{"1Address":0.5}`)
			},
			staticCmd: func() interface{} {
				amounts := map[string]float64{"1Address": 0.5}
				return bsvjson.NewSendManyCmd("from", amounts, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendmany","params":["from",{"1Address":0.5}],"id":1}`,
			unmarshalled: &bsvjson.SendManyCmd{
				FromAccount: "from",
				Amounts:     map[string]float64{"1Address": 0.5},
				MinConf:     bsvjson.Int(1),
				Comment:     nil,
			},
		},
		{
			name: "sendmany optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendmany", "from", `{"1Address":0.5}`, 6)
			},
			staticCmd: func() interface{} {
				amounts := map[string]float64{"1Address": 0.5}
				return bsvjson.NewSendManyCmd("from", amounts, bsvjson.Int(6), nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendmany","params":["from",{"1Address":0.5},6],"id":1}`,
			unmarshalled: &bsvjson.SendManyCmd{
				FromAccount: "from",
				Amounts:     map[string]float64{"1Address": 0.5},
				MinConf:     bsvjson.Int(6),
				Comment:     nil,
			},
		},
		{
			name: "sendmany optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendmany", "from", `{"1Address":0.5}`, 6, "comment")
			},
			staticCmd: func() interface{} {
				amounts := map[string]float64{"1Address": 0.5}
				return bsvjson.NewSendManyCmd("from", amounts, bsvjson.Int(6), bsvjson.String("comment"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendmany","params":["from",{"1Address":0.5},6,"comment"],"id":1}`,
			unmarshalled: &bsvjson.SendManyCmd{
				FromAccount: "from",
				Amounts:     map[string]float64{"1Address": 0.5},
				MinConf:     bsvjson.Int(6),
				Comment:     bsvjson.String("comment"),
			},
		},
		{
			name: "sendtoaddress",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendtoaddress", "1Address", 0.5)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendToAddressCmd("1Address", 0.5, nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendtoaddress","params":["1Address",0.5],"id":1}`,
			unmarshalled: &bsvjson.SendToAddressCmd{
				Address:               "1Address",
				Amount:                0.5,
				Comment:               nil,
				CommentTo:             nil,
				SubtractFeeFromAmount: nil,
			},
		},
		{
			name: "sendtoaddress optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("sendtoaddress", "1Address", 0.5, "comment", "commentto", true)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSendToAddressCmd("1Address", 0.5, bsvjson.String("comment"),
					bsvjson.String("commentto"), bsvjson.Bool(true))
			},
			marshalled: `{"jsonrpc":"1.0","method":"sendtoaddress","params":["1Address",0.5,"comment","commentto",true],"id":1}`,
			unmarshalled: &bsvjson.SendToAddressCmd{
				Address:               "1Address",
				Amount:                0.5,
				Comment:               bsvjson.String("comment"),
				CommentTo:             bsvjson.String("commentto"),
				SubtractFeeFromAmount: bsvjson.Bool(true),
			},
		},
		{
			name: "setaccount",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("setaccount", "1Address", "acct")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSetAccountCmd("1Address", "acct")
			},
			marshalled: `{"jsonrpc":"1.0","method":"setaccount","params":["1Address","acct"],"id":1}`,
			unmarshalled: &bsvjson.SetAccountCmd{
				Address: "1Address",
				Account: "acct",
			},
		},
		{
			name: "settxfee",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("settxfee", 0.0001)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSetTxFeeCmd(0.0001)
			},
			marshalled: `{"jsonrpc":"1.0","method":"settxfee","params":[0.0001],"id":1}`,
			unmarshalled: &bsvjson.SetTxFeeCmd{
				Amount: 0.0001,
			},
		},
		{
			name: "signmessage",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("signmessage", "1Address", "message")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSignMessageCmd("1Address", "message")
			},
			marshalled: `{"jsonrpc":"1.0","method":"signmessage","params":["1Address","message"],"id":1}`,
			unmarshalled: &bsvjson.SignMessageCmd{
				Address: "1Address",
				Message: "message",
			},
		},
		{
			name: "signrawtransaction",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("signrawtransaction", "001122")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewSignRawTransactionCmd("001122", nil, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"signrawtransaction","params":["001122"],"id":1}`,
			unmarshalled: &bsvjson.SignRawTransactionCmd{
				RawTx:    "001122",
				Inputs:   nil,
				PrivKeys: nil,
				Flags:    bsvjson.String("ALL"),
			},
		},
		{
			name: "signrawtransaction optional1",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("signrawtransaction", "001122", `[{"txid":"123","vout":1,"scriptPubKey":"00","redeemScript":"01","amount":0.0001}]`)
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.RawTxInput{
					{
						Txid:         "123",
						Vout:         1,
						ScriptPubKey: "00",
						RedeemScript: "01",
						Amount:       0.0001,
					},
				}

				return bsvjson.NewSignRawTransactionCmd("001122", &txInputs, nil, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"signrawtransaction","params":["001122",[{"txid":"123","vout":1,"scriptPubKey":"00","redeemScript":"01","amount":0.0001}]],"id":1}`,
			unmarshalled: &bsvjson.SignRawTransactionCmd{
				RawTx: "001122",
				Inputs: &[]bsvjson.RawTxInput{
					{
						Txid:         "123",
						Vout:         1,
						ScriptPubKey: "00",
						RedeemScript: "01",
						Amount:       0.0001,
					},
				},
				PrivKeys: nil,
				Flags:    bsvjson.String("ALL"),
			},
		},
		{
			name: "signrawtransaction optional2",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("signrawtransaction", "001122", `[]`, `["abc"]`)
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.RawTxInput{}
				privKeys := []string{"abc"}
				return bsvjson.NewSignRawTransactionCmd("001122", &txInputs, &privKeys, nil)
			},
			marshalled: `{"jsonrpc":"1.0","method":"signrawtransaction","params":["001122",[],["abc"]],"id":1}`,
			unmarshalled: &bsvjson.SignRawTransactionCmd{
				RawTx:    "001122",
				Inputs:   &[]bsvjson.RawTxInput{},
				PrivKeys: &[]string{"abc"},
				Flags:    bsvjson.String("ALL"),
			},
		},
		{
			name: "signrawtransaction optional3",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("signrawtransaction", "001122", `[]`, `[]`, "ALL")
			},
			staticCmd: func() interface{} {
				txInputs := []bsvjson.RawTxInput{}
				privKeys := []string{}
				return bsvjson.NewSignRawTransactionCmd("001122", &txInputs, &privKeys,
					bsvjson.String("ALL"))
			},
			marshalled: `{"jsonrpc":"1.0","method":"signrawtransaction","params":["001122",[],[],"ALL"],"id":1}`,
			unmarshalled: &bsvjson.SignRawTransactionCmd{
				RawTx:    "001122",
				Inputs:   &[]bsvjson.RawTxInput{},
				PrivKeys: &[]string{},
				Flags:    bsvjson.String("ALL"),
			},
		},
		{
			name: "walletlock",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("walletlock")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewWalletLockCmd()
			},
			marshalled:   `{"jsonrpc":"1.0","method":"walletlock","params":[],"id":1}`,
			unmarshalled: &bsvjson.WalletLockCmd{},
		},
		{
			name: "walletpassphrase",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("walletpassphrase", "pass", 60)
			},
			staticCmd: func() interface{} {
				return bsvjson.NewWalletPassphraseCmd("pass", 60)
			},
			marshalled: `{"jsonrpc":"1.0","method":"walletpassphrase","params":["pass",60],"id":1}`,
			unmarshalled: &bsvjson.WalletPassphraseCmd{
				Passphrase: "pass",
				Timeout:    60,
			},
		},
		{
			name: "walletpassphrasechange",
			newCmd: func() (interface{}, error) {
				return bsvjson.NewCmd("walletpassphrasechange", "old", "new")
			},
			staticCmd: func() interface{} {
				return bsvjson.NewWalletPassphraseChangeCmd("old", "new")
			},
			marshalled: `{"jsonrpc":"1.0","method":"walletpassphrasechange","params":["old","new"],"id":1}`,
			unmarshalled: &bsvjson.WalletPassphraseChangeCmd{
				OldPassphrase: "old",
				NewPassphrase: "new",
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
