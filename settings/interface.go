package settings

import (
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
)

type KafkaSettings struct {
	Blocks            string
	BlocksFinal       string
	BlocksValidate    string
	Hosts             string
	LegacyInv         string
	Partitions        int
	Port              int
	RejectedTx        string
	ReplicationFactor int
	Subtrees          string
	TxMeta            string
	UnitTest          string
	ValidatorTxs      string
}

type AerospikeSettings struct {
	Debug                  bool
	Host                   string
	BatchPolicy            string
	ReadPolicy             string
	WritePolicy            string
	Port                   int
	UseDefaultBasePolicies bool
	UseDefaultPolicies     bool
	WarmUp                 bool
}

type AlertSettings struct {
	GenesisKeys   []string
	P2PPrivateKey string
	ProtocolID    string
	Store         string
	StoreURL      *url.URL
	TopicName     string
	P2PPort       int
}

type AssetSettings struct {
	APIPrefix               string
	CentrifugeListenAddress string
	CentrifugeDisable       bool
	HTTPAddress             string
	HTTPListenAddress       string
	HTTPPort                int
	HTTPSPort               int
}

type BlockSettings struct {
	MinedCacheMaxMB                       int
	PersisterStore                        string
	PersisterHTTPListenAddress            string
	StateFile                             string
	CheckDuplicateTransactionsConcurrency int
	GetAndValidateSubtreesConcurrency     int
	KafkaWorkers                          int
	ValidOrderAndBlessedConcurrency       int
	StoreCacheEnabled                     bool
	MaxSize                               int
	StorePath                             string
	FailFastValidation                    bool
	FinalizeBlockValidationConcurrency    int
	GetMissingTransactions                int
}

type BlockChainSettings struct {
	GRPCAddress       string
	GRPCListenAddress string
	HTTPListenAddress string
	MaxRetries        int
	StoreURL          *url.URL
	FSMStateRestore   bool
}

type BlockAssemblySettings struct {
	Disabled                            bool
	GRPCAddress                         string
	GRPCListenAddress                   string
	GRPCMaxRetries                      int
	GRPCRetryBackoff                    string
	LocalTTLCache                       string
	MaxBlockReorgCatchup                int
	MaxBlockReorgRollback               int
	MoveDownBlockConcurrency            int
	ProcessRemainderTxHashesConcurrency int
	SendBatchSize                       int
	SendBatchTimeout                    int
	SubtreeProcessorBatcherSize         int
	SubtreeProcessorConcurrentReads     int
	SubtreeTTL                          time.Duration
	NewSubtreeChanBuffer                int
	SubtreeRetryChanBuffer              int
	SubmitMiningSolutionWaitForResponse bool
}

type BlockValidationSettings struct {
	MaxRetries                     int
	RetrySleep                     string
	GRPCAddress                    string
	GRPCListenAddress              string
	HTTPAddress                    string
	HTTPListenAddress              string
	KafkaWorkers                   int
	LocalSetTxMinedConcurrency     int
	MaxPreviousBlockHeadersToCheck int
}

type ValidatorSettings struct {
	GrpcAddress               string
	GrpcListenAddress         string
	KafkaWorkers              int
	ScriptVerificationLibrary string
	SendBatchSize             int
	SendBatchTimeout          int
	SendBatchWorkers          int
	GrpcPort                  int
	BlockValidationDelay      int
	BlockValidationMaxRetries int
	BlockValidationRetrySleep string
}

type RedisSettings struct {
	Hosts string
	Port  int
}

type RegionSettings struct {
	Name string
}

type AdvertisingSettings struct {
	Interval string
	URL      string
}

type UtxoStoreSettings struct {
	OutpointBatcherSize        int
	SpendBatcherConcurrency    int
	SpendBatcherDurationMillis int
	SpendBatcherSize           int
	StoreBatcherConcurrency    int
	StoreBatcherDurationMillis int
	StoreBatcherSize           int
	UtxoBatchSize              int
}

type Settings struct {
	ClientName      string
	DataFolder      string
	ChainCfgParams  *chaincfg.Params
	Policy          *PolicySettings
	Kafka           KafkaSettings
	Aerospike       AerospikeSettings
	Alert           AlertSettings
	Asset           AssetSettings
	Block           BlockSettings
	BlockAssembly   BlockAssemblySettings
	BlockChain      BlockChainSettings
	BlockValidation BlockValidationSettings
	Validator       ValidatorSettings
	Redis           RedisSettings
	Region          RegionSettings
	Advertising     AdvertisingSettings
	UtxoStore       UtxoStoreSettings
}
