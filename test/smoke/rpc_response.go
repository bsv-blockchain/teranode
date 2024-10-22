package test

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
	Error  interface{} `json:"error"`
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
	Error interface{} `json:"error"`
	ID    int         `json:"id"`
}

type GetDifficultyResponse struct {
	Result float64     `json:"result"`
	Error  interface{} `json:"error"`
	ID     int         `json:"id"`
}
