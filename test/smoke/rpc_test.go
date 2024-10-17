////go:build rpc

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

func TestRPCTestSuite(t *testing.T) {
	suite.Run(t, new(RPCTestSuite))
}
