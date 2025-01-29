package settings

import (
	"net/url"
	"time"

	"github.com/bitcoin-sv/teranode/chaincfg"
)

type Settings struct {
	ServiceName              string
	ClientName               string
	DataFolder               string
	SecurityLevelHTTP        int
	ServerCertFile           string
	ServerKeyFile            string
	Logger                   string
	LogLevel                 string
	PrettyLogs               bool
	ProfilerAddr             string
	StatsPrefix              string
	PrometheusEndpoint       string
	HealthCheckPort          int
	UseDatadogProfiler       bool
	TracingSampleRate        string
	LocalTestStartFromState  string
	PostgresCheckAddress     string
	UseCgoVerifier           bool
	SecurityLevelGRPC        int
	UsePrometheusGRPCMetrics bool
	TracingCollectorURL      *url.URL

	ChainCfgParams    *chaincfg.Params
	Policy            *PolicySettings
	Kafka             KafkaSettings
	Aerospike         AerospikeSettings
	Alert             AlertSettings
	Asset             AssetSettings
	Block             BlockSettings
	BlockAssembly     BlockAssemblySettings
	BlockChain        BlockChainSettings
	BlockValidation   BlockValidationSettings
	Validator         ValidatorSettings
	Redis             RedisSettings
	Region            RegionSettings
	Advertising       AdvertisingSettings
	UtxoStore         UtxoStoreSettings
	P2P               P2PSettings
	Coinbase          CoinbaseSettings
	SubtreeValidation SubtreeValidationSettings
	Legacy            LegacySettings
	Propagation       PropagationSettings
	RPC               RPCSettings
	Faucet            FaucetSettings
	Dashboard         DashboardSettings
	UseOpenTracing    bool
	UseOtelTracing    bool
}

type DashboardSettings struct {
	Enabled bool
}

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
	TxMetaConfig      *url.URL
	LegacyInvConfig   *url.URL
	BlocksFinalConfig *url.URL
}

type AerospikeSettings struct {
	Debug                  bool
	Host                   string
	BatchPolicyURL         *url.URL
	ReadPolicyURL          *url.URL
	WritePolicyURL         *url.URL
	Port                   int
	UseDefaultBasePolicies bool
	UseDefaultPolicies     bool
	WarmUp                 bool
	StoreBatcherDuration   time.Duration
	StatsRefreshDuration   time.Duration
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
	SignHTTPResponses       bool
	EchoDebug               bool
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
	StoreCacheSize                        int
	MaxSize                               int
	BlockStore                            *url.URL
	FailFastValidation                    bool
	FinalizeBlockValidationConcurrency    int
	GetMissingTransactions                int
	QuorumTimeout                         time.Duration
	BlockPersisterConcurrency             int
	BatchMissingTransactions              bool
	ProcessTxMetaUsingStoreBatchSize      int
	SkipUTXODelete                        bool
	UTXOPersisterBufferSize               string
	TxStore                               *url.URL
	UTXOPersisterDirect                   bool
	BlockPersisterPersistAge              uint32
	BlockPersisterPersistSleep            time.Duration
	TxMetaStore                           *url.URL
}

type BlockChainSettings struct {
	GRPCAddress           string
	GRPCListenAddress     string
	HTTPListenAddress     string
	MaxRetries            int
	RetrySleep            int
	StoreURL              *url.URL
	FSMStateRestore       bool
	FSMStateChangeDelay   time.Duration // used by tests to delay the state change and have time to capture the state
	StoreDBTimeoutMillis  int
	InitializeNodeInState string
}

type BlockAssemblySettings struct {
	Disabled                            bool
	GRPCAddress                         string
	GRPCListenAddress                   string
	GRPCMaxRetries                      int
	GRPCRetryBackoff                    time.Duration
	LocalTTLCache                       string
	MaxBlockReorgCatchup                int
	MaxBlockReorgRollback               int
	MoveBackBlockConcurrency            int
	ProcessRemainderTxHashesConcurrency int
	SendBatchSize                       int
	SendBatchTimeout                    int
	SubtreeProcessorBatcherSize         int
	SubtreeProcessorConcurrentReads     int
	SubtreeTTL                          time.Duration
	NewSubtreeChanBuffer                int
	SubtreeRetryChanBuffer              int
	SubmitMiningSolutionWaitForResponse bool
	InitialMerkleItemsPerSubtree        int
	DoubleSpendWindow                   time.Duration
	MaxGetReorgHashes                   int
	MinerWalletPrivateKeys              []string
	DifficultyCache                     bool
}

type BlockValidationSettings struct {
	MaxRetries                                int
	RetrySleep                                time.Duration
	GRPCAddress                               string
	GRPCListenAddress                         string
	HTTPAddress                               string
	HTTPListenAddress                         string
	KafkaWorkers                              int
	LocalSetTxMinedConcurrency                int
	MaxPreviousBlockHeadersToCheck            uint64
	MissingTransactionsBatchSize              int
	ProcessTxMetaUsingCacheBatchSize          int
	ProcessTxMetaUsingCacheConcurrency        int
	ProcessTxMetaUsingCacheMissingTxThreshold int
	ProcessTxMetaUsingStoreBatchSize          int
	ProcessTxMetaUsingStoreConcurrency        int
	ProcessTxMetaUsingStoreMissingTxThreshold int
	SkipCheckParentMined                      bool
	SubtreeFoundChConcurrency                 int
	SubtreeTTL                                time.Duration
	SubtreeTTLConcurrency                     int
	SubtreeValidationAbandonThreshold         int
	ValidateBlockSubtreesConcurrency          int
	ValidationMaxRetries                      int
	ValidationRetrySleep                      time.Duration
	OptimisticMining                          bool
	IsParentMinedRetryMaxRetry                int
	IsParentMinedRetryBackoffMultiplier       int
	SubtreeGroupConcurrency                   int
	BlockFoundChBufferSize                    int
	CatchupChBufferSize                       int
	UseCatchupWhenBehind                      bool
	CatchupConcurrency                        int
	ValidationWarmupCount                     int
	BatchMissingTransactions                  bool
}

type ValidatorSettings struct {
	GRPCAddress               string
	GRPCListenAddress         string
	KafkaWorkers              int
	SendBatchSize             int
	SendBatchTimeout          int
	SendBatchWorkers          int
	BlockValidationDelay      int
	BlockValidationMaxRetries int
	BlockValidationRetrySleep string
	VerboseDebug              bool
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
	UtxoStore                      *url.URL
	OutpointBatcherSize            int
	OutpointBatcherDurationMillis  int
	SpendBatcherConcurrency        int
	SpendBatcherDurationMillis     int
	SpendBatcherSize               int
	StoreBatcherConcurrency        int
	StoreBatcherDurationMillis     int
	StoreBatcherSize               int
	UtxoBatchSize                  int
	IncrementBatcherSize           int
	IncrementBatcherDurationMillis int
	GetBatcherSize                 int
	GetBatcherDurationMillis       int
	DBTimeout                      time.Duration
	UseExternalTxCache             bool
	ExternalizeAllTransactions     bool
	PostgresMaxIdleConns           int
	PostgresMaxOpenConns           int
	VerboseDebug                   bool
	UpdateTxMinedStatus            bool
	MaxMinedRoutines               int
	MaxMinedBatchSize              int
}

type P2PSettings struct {
	BestBlockTopic     string
	BlockTopic         string
	BootstrapAddresses []string

	GRPCAddress       string
	GRPCListenAddress string

	HTTPAddress       string
	HTTPListenAddress string

	IP            string
	MiningOnTopic string

	PeerID string
	Port   int

	PrivateKey      string
	RejectedTxTopic string

	SharedKey   string
	StaticPeers []string

	SubtreeTopic string
	TopicPrefix  string

	DHTProtocolID   string
	DHTUsePrivate   bool
	OptimiseRetries bool
}

type CoinbaseSettings struct {
	DB                          string
	UserPwd                     string
	ArbitraryText               string
	GRPCAddress                 string
	GRPCListenAddress           string
	NotificationThreshold       int
	P2PPeerID                   string
	P2PPrivateKey               string
	P2PStaticPeers              []string
	ShouldWait                  bool
	Store                       *url.URL
	StoreDBTimeoutMillis        int
	WaitForPeers                bool
	WalletPrivateKey            string
	DistributorBackoffDuration  time.Duration
	DistributorMaxRetries       int
	DistributorFailureTolerance int
	DistributerWaitTime         int
	DistributorTimeout          time.Duration
	PeerStatusTimeout           time.Duration
	SlackChannel                string
	SlackToken                  string
	TestMode                    bool
	P2PPort                     int
}

type SubtreeValidationSettings struct {
	QuorumPath                                string
	QuorumAbsoluteTimeout                     time.Duration
	SubtreeStore                              *url.URL
	FailFastValidation                        bool
	GetMissingTransactions                    int
	GRPCAddress                               string
	GRPCListenAddress                         string
	ProcessTxMetaUsingCacheBatchSize          int
	ProcessTxMetaUsingCacheConcurrency        int
	ProcessTxMetaUsingCacheMissingTxThreshold int
	ProcessTxMetaUsingStoreBatchSize          int
	ProcessTxMetaUsingStoreConcurrency        int
	ProcessTxMetaUsingStoreMissingTxThreshold int
	SubtreeFoundChConcurrency                 int
	SubtreeTTL                                time.Duration
	SubtreeTTLConcurrency                     int
	SubtreeValidationTimeout                  int
	SubtreeValidationAbandonThreshold         int
	TxMetaCacheEnabled                        bool
	TxMetaCacheMaxMB                          int
	ValidationMaxRetries                      int
	ValidationRetrySleep                      string
	TxChanBufferSize                          int
	BatchMissingTransactions                  bool
	SpendBatcherSize                          int
	MissingTransactionsBatchSize              int
}

type LegacySettings struct {
	ListenAddresses                  []string
	ConnectPeers                     []string
	OrphanEvictionDuration           time.Duration
	StoreBatcherSize                 int
	StoreBatcherConcurrency          int
	SpendBatcherSize                 int
	SpendBatcherConcurrency          int
	OutpointBatcherSize              int
	OutpointBatcherConcurrency       int
	PrintInvMessages                 bool
	GRPCAddress                      string
	AllowBlockPriority               bool
	WriteMsgBlocksToDisk             bool // Write blocks to disk when syncing with other nodes, this reduces the memory footprint
	LimitedBlockValidation           bool
	GRPCListenAddress                string
	SavePeers                        bool
	AllowSyncCandidateFromLocalPeers bool
}

type PropagationSettings struct {
	IPv6Addresses        string
	IPv6Interface        string
	QuicListenAddress    string
	GRPCMaxConnectionAge time.Duration
	HTTPListenAddress    string
	SendBatchSize        int
	SendBatchTimeout     int
	GRPCResolver         string
	GRPCAddresses        []string
	QuicAddresses        []string
	GRPCListenAddress    string
	UseDumb              bool
}

type RPCSettings struct {
	RPCUser        string
	RPCPass        string
	RPCLimitUser   string
	RPCLimitPass   string
	RPCMaxClients  int
	RPCQuirks      bool
	RPCListenerURL string
}

type FaucetSettings struct {
	HTTPListenAddress string
}
