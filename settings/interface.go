package settings

import (
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
)

type Settings struct {
	ClientName        string
	DataFolder        string
	SecurityLevelHTTP int
	ServerCertFile    string
	ServerKeyFile     string
	LogLevel          string
	ProfilerAddr      string
	StatsPrefix       string

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
	BatchPolicy            string
	ReadPolicy             string
	WritePolicy            string
	Port                   int
	UseDefaultBasePolicies bool
	UseDefaultPolicies     bool
	WarmUp                 bool
	StoreBatcherDuration   time.Duration
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
}

type BlockChainSettings struct {
	GRPCAddress          string
	GRPCListenAddress    string
	HTTPListenAddress    string
	MaxRetries           int
	RetrySleep           int
	StoreURL             *url.URL
	FSMStateRestore      bool
	FSMStateChangeDelay  time.Duration // used by tests to delay the state change and have time to capture the state
	StoreDBTimeoutMillis int
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
	InitialMerkleItemsPerSubtree        int
	DoubleSpendWindow                   time.Duration
	MaxGetReorgHashes                   int
	MinerWalletPrivateKeys              []string
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
	ScriptVerificationLibrary string
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
	UtxoStore                  *url.URL
	OutpointBatcherSize        int
	SpendBatcherConcurrency    int
	SpendBatcherDurationMillis int
	SpendBatcherSize           int
	StoreBatcherConcurrency    int
	StoreBatcherDurationMillis int
	StoreBatcherSize           int
	UtxoBatchSize              int
	GetBatcherSize             int
	DBTimeout                  time.Duration
	UseExternalTxCache         bool
	ExternalizeAllTransactions bool
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

	PeerID       string
	Port         int
	PortCoinbase int

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
	PeerStatusTimeout           time.Duration
	SlackChannel                string
	SlackToken                  string
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
	ValidationMaxRetries                      int
	ValidationRetrySleep                      string
	TxChanBufferSize                          int
	BatchMissingTransactions                  bool
	SpendBatcherSize                          int
	MissingTransactionsBatchSize              int
}

type LegacySettings struct {
	ListenAddresses            []string
	ConnectPeers               []string
	OrphanEvictionDuration     time.Duration
	StoreBatcherSize           int
	StoreBatcherConcurrency    int
	SpendBatcherSize           int
	SpendBatcherConcurrency    int
	OutpointBatcherSize        int
	OutpointBatcherConcurrency int
	PrintInvMessages           bool
	GRPCAddress                string
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
