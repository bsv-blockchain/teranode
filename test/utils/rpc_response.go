package utils

import "fmt"

type JSONError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (je *JSONError) Error() string {
	return fmt.Sprintf("code: %d, message: %s", je.Code, je.Message)
}

type BlockchainInfo struct {
	Result struct {
		BestBlockHash        string   `json:"bestblockhash"`
		Blocks               int      `json:"blocks"`
		Chain                string   `json:"chain"`
		Chainwork            string   `json:"chainwork"`
		Difficulty           float64  `json:"difficulty"`
		Headers              int      `json:"headers"`
		Mediantime           int      `json:"mediantime"`
		Pruned               bool     `json:"pruned"`
		Softforks            []string `json:"softforks"`
		VerificationProgress float64  `json:"verificationprogress"`
	} `json:"result"`
	Error *JSONError  `json:"error"`
	ID    interface{} `json:"id"`
}

type StreamInfoNode struct {
	StreamType int `json:"stream_type"`
	BytesSent  int `json:"bytessent"`
	BytesRecv  int `json:"bytesrecv"`
}

type P2PNode struct {
	ID             int              `json:"id"`
	Addr           string           `json:"addr"`
	AddrLocal      string           `json:"addrlocal"`
	Services       string           `json:"services"`
	ServicesStr    string           `json:"servicesStr"`
	RelayTxes      bool             `json:"relaytxes"`
	LastSend       int64            `json:"lastsend"`
	LastRecv       int64            `json:"lastrecv"`
	BytesSent      int              `json:"bytessent"`
	BytesRecv      int              `json:"bytesrecv"`
	ConnTime       int64            `json:"conntime"`
	TimeOffset     int              `json:"timeoffset"`
	PingTime       int              `json:"pingtime"`
	Version        int              `json:"version"`
	SubVer         string           `json:"subver"`
	Inbound        bool             `json:"inbound"`
	StartingHeight int              `json:"startingheight"`
	BanScore       int              `json:"banscore"`
	Whitelisted    bool             `json:"whitelisted"`
	FeeFilter      int              `json:"feefilter"`
	SyncNode       bool             `json:"syncnode"`
	Streams        []StreamInfoNode `json:"streams,omitempty"`
}

type P2PRPCResponse struct {
	Result []P2PNode   `json:"result"`
	Error  *JSONError  `json:"error"`
	ID     interface{} `json:"id"`
}

type GetInfo struct {
	Result struct {
		Blocks          int     `json:"blocks"`
		Connections     int     `json:"connections"`
		Difficulty      float64 `json:"difficulty"`
		ProtocolVersion int     `json:"protocolversion"`
		Proxy           string  `json:"proxy"`
		RelayFee        float64 `json:"relayfee"`
		Stn             bool    `json:"stn"`
		TestNet         bool    `json:"testnet"`
		TimeOffset      int     `json:"timeoffset"`
		Version         int     `json:"version"`
	} `json:"result"`
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type GetDifficultyResponse struct {
	Result float64    `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetBlockHashResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetBlockByHeightStatus struct {
	Validity            string `json:"validity"`
	Data                bool   `json:"data"`
	Undo                bool   `json:"undo"`
	Failed              bool   `json:"failed"`
	ParentFailed        bool   `json:"parent failed"`
	DiskMeta            bool   `json:"disk meta"`
	SoftReject          bool   `json:"soft reject"`
	DoubleSpend         bool   `json:"double spend"`
	SoftConsensusFrozen bool   `json:"soft consensus frozen"`
}

type GetBlockByHeightResponse struct {
	Result struct {
		Tx                []string               `json:"tx"`
		Hash              string                 `json:"hash"`
		Confirmations     int                    `json:"confirmations"`
		Size              int                    `json:"size"`
		Height            uint32                 `json:"height"`
		Version           int                    `json:"version"`
		VersionHex        string                 `json:"versionHex"`
		Merkleroot        string                 `json:"merkleroot"`
		NumTx             int                    `json:"num_tx"`
		Time              int64                  `json:"time"`
		Mediantime        int64                  `json:"mediantime"`
		Nonce             int                    `json:"nonce"`
		Bits              string                 `json:"bits"`
		Difficulty        float64                `json:"difficulty"`
		Chainwork         string                 `json:"chainwork"`
		Previousblockhash string                 `json:"previousblockhash"`
		Nextblockhash     string                 `json:"nextblockhash"`
		Status            GetBlockByHeightStatus `json:"status"`
	}
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type GetMiningInfoResponse struct {
	Result struct {
		Blocks           int     `json:"blocks"`
		CurrentBlockSize int     `json:"currentblocksize"`
		CurrentBlockTx   int     `json:"currentblocktx"`
		Difficulty       float64 `json:"difficulty"`
		Errors           string  `json:"errors"`
		NetworkHashPs    float64 `json:"networkhashps"`
		Chain            string  `json:"chain"`
	}
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type MiningCandidate struct {
	Result struct {
		ID                  string   `json:"id"`
		PreviousHash        string   `json:"prevhash"`
		Coinbase            string   `json:"coinbase"`
		CoinbaseValue       uint64   `json:"coinbaseValue"`
		Version             uint32   `json:"version"`
		NBits               string   `json:"nBits"`
		Time                uint32   `json:"time"`
		Height              uint32   `json:"height"`
		MerkleProof         []string `json:"merkleProof"`
		NumTxs              uint32   `json:"num_txs"`
		SizeWithoutCoinbase uint64   `json:"sizeWithoutCoinbase"`
	}
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type InvalidBlockResp struct {
	Result interface{} `json:"result"`
	Error  *JSONError  `json:"error"`
	ID     int         `json:"id"`
}

type BestBlockHashResp struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type CreateRawTransactionResp struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetRawTransactionResponse struct {
	Result struct {
		Hex      string `json:"hex"`
		Txid     string `json:"txid"`
		Hash     string `json:"hash"`
		Size     int32  `json:"size"`
		Version  int32  `json:"version"`
		LockTime uint32 `json:"locktime"`
	}
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type ChainTxStatsResponse struct {
	Result struct {
		Time             int64   `json:"time"`
		TxCount          int     `json:"txcount"`
		WindowBlockCount int     `json:"window_block_count"`
		WindowTxCount    int     `json:"window_tx_count"`
		WindowInterval   int64   `json:"window_interval"`
		TxRate           float64 `json:"txrate"`
	} `json:"result"`
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type TxOutSetInfoResponse struct {
	Result struct {
		Height       int     `json:"height"`
		BestBlock    string  `json:"bestblock"`
		Transactions int     `json:"transactions"`
		TxOuts       int     `json:"txouts"`
		BogoSize     int     `json:"bogosize"`
		TotalAmount  float64 `json:"total_amount"`
		DiskSize     int64   `json:"disk_size"`
	} `json:"result"`
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type NetworkHashPSResponse struct {
	Result float64    `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetBlockHexResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type BlockHeaderResponse struct {
	Result struct {
		Hash              string  `json:"hash"`
		Confirmations     int     `json:"confirmations"`
		Height            uint32  `json:"height"`
		Version           int     `json:"version"`
		VersionHex        string  `json:"versionHex"`
		Merkleroot        string  `json:"merkleroot"`
		Time              int64   `json:"time"`
		Mediantime        int64   `json:"mediantime"`
		Nonce             uint32  `json:"nonce"`
		Bits              string  `json:"bits"`
		Difficulty        float64 `json:"difficulty"`
		Chainwork         string  `json:"chainwork"`
		Previousblockhash string  `json:"previousblockhash"`
		Nextblockhash     string  `json:"nextblockhash"`
	} `json:"result"`
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type GetBlockHeaderHexResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type UptimeResponse struct {
	Result int64      `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type ValidateAddressResponse struct {
	Result struct {
		IsValid bool   `json:"isvalid"`
		Address string `json:"address,omitempty"`
	} `json:"result"`
	Error *JSONError `json:"error"`
	ID    int        `json:"id"`
}

type GenerateToAddressResponse struct {
	Result []string   `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type CreateRawTransactionResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetRawTransactionHexResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type HelpResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetBlockCountResponse struct {
	Result int        `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}

type GetBestBlockHashResponse struct {
	Result string     `json:"result"`
	Error  *JSONError `json:"error"`
	ID     int        `json:"id"`
}
