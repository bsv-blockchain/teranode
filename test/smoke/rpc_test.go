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

type BlockchainInfo struct {
	Result struct {
		BestBlockHash        string   `json:"bestblockhash"`
		Blocks               int      `json:"blocks"`
		Chain                string   `json:"chain"`
		Chainwork            string   `json:"chainwork"`
		Difficulty           string   `json:"difficulty"`
		Headers              int      `json:"headers"`
		Mediantime           int      `json:"mediantime"`
		Pruned               bool     `json:"pruned"`
		Softforks            []string `json:"softforks"`
		VerificationProgress float64  `json:"verificationprogress"`
	} `json:"result"`
	Error interface{} `json:"error"`
	ID    interface{} `json:"id"`
}

type P2PNode struct {
	ID             int    `json:"id"`
	Addr           string `json:"addr"`
	Services       string `json:"services"`
	ServicesStr    string `json:"servicesStr"`
	RelayTxes      bool   `json:"relaytxes"`
	LastSend       int    `json:"lastsend"`
	LastRecv       int    `json:"lastrecv"`
	BytesSent      int    `json:"bytessent"`
	BytesRecv      int    `json:"bytesrecv"`
	ConnTime       int    `json:"conntime"`
	TimeOffset     int    `json:"timeoffset"`
	PingTime       int    `json:"pingtime"`
	Version        int    `json:"version"`
	SubVer         string `json:"subver"`
	Inbound        bool   `json:"inbound"`
	StartingHeight int    `json:"startingheight"`
	BanScore       int    `json:"banscore"`
	Whitelisted    bool   `json:"whitelisted"`
	FeeFilter      int    `json:"feefilter"`
	SyncNode       bool   `json:"syncnode"`
}

type P2PRPCResponse struct {
	Result []P2PNode   `json:"result"`
	Error  interface{} `json:"error"` // `error` potrebbe essere null o contenere informazioni, quindi usa `interface{}`
	ID     interface{} `json:"id"`    // `id` potrebbe essere null o un numero, quindi `interface{}`
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

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
}
