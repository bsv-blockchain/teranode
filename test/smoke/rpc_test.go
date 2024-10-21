//go:build rpc

// go test -v -run "^TestRPCTestSuite$/TestRPCGetBlockchainInfo$" -tags rpc

package test

import (
	"encoding/json"
	"testing"

	arrange "github.com/bitcoin-sv/ubsv/test/fixtures"
	helper "github.com/bitcoin-sv/ubsv/test/utils"
	"github.com/stretchr/testify/suite"
)

type RPCTestSuite struct {
	arrange.TeranodeTestSuite
}

const (
	ubsv1RPCEndpoint string = "http://localhost:11292"
)

func (suite *RPCTestSuite) TestRPCGetBlockchainInfo() {
	var blockchainInfo BlockchainInfo

	t := suite.T()
	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getblockchaininfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &blockchainInfo)

	if errJSON != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if blockchainInfo.Result.BestBlockHash == "" {
		t.Errorf("Test failed: BestBlockHash is empty")
	} else {
		t.Logf("Test succeeded: BestBlockHash is not empty")
	}
}

func (suite *RPCTestSuite) TestRPCGetPeerInfo() {
	t := suite.T()

	var p2pResp P2PRPCResponse

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getpeerinfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &p2pResp)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if len(p2pResp.Result) == 0 {
		t.Errorf("Test failed: peers list is empty")
	} else {
		t.Logf("Test succeeded, retrieved P2P peers informations")
	}
}

func (suite *RPCTestSuite) TestRPCGetInfo() {
	t := suite.T()

	var getInfo GetInfo

	resp, err := helper.CallRPC(ubsv1RPCEndpoint, "getinfo", []interface{}{})

	if err != nil {
		t.Errorf("Error CallRPC: %v", err)
	}

	errJSON := json.Unmarshal([]byte(resp), &getInfo)
	if err != nil {
		t.Errorf("JSON decoding error: %v", errJSON)
		return
	}

	t.Logf("%s", resp)

	if getInfo.Error != nil {
		if strErr, ok := getInfo.Error.(string); ok && strErr == "null" {
			t.Errorf("Test failed: getinfo RPC call returned error: %v", strErr)
		} else {
			t.Errorf("Test failed: getinfo RPC call returned an unexpected error type: %v", getInfo.Error)
		}
	} else {
		t.Logf("Test succeeded, retrieved information from getinfo RPC call")
	}
}

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
}
