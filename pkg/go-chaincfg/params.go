// Copyright (c) 2014-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package chaincfg

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/pkg/go-wire"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// These variables are the chain proof-of-work limit parameters for each default
// network.
var (
	// bigOne is 1 represented as a big.Int.  It is defined here to avoid
	// the overhead of creating it multiple times.
	bigOne = big.NewInt(1)

	// mainPowLimit is the highest proof of work value a Bitcoin block can
	// have for the main network.  It is the value 2^224 - 1.
	mainPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)

	// regressionPowLimit is the highest proof of work value a Bitcoin block
	// can have for the regression test network.  It is equal to (2^{255} - 1).
	regressionPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 255), bigOne)

	// testNetPowLimit is the highest proof of work value a Bitcoin block
	// can have for the test network (version 3).  It is the value
	// 2^224 - 1.
	testNetPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)

	// stnPowLimit is the highest proof of work value a Bitcoin block can
	// have for the scaling test network. It is the value 2^224 - 1.
	stnPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)
)

// Checkpoint represents a block height and hash pair
type Checkpoint struct {
	Height int32
	Hash   *chainhash.Hash
}

// DNSSeed identifies a DNS seed.
type DNSSeed struct {
	// Host defines the hostname of the seed.
	Host string

	// HasFiltering defines whether the seed supports filtering
	// by service flags (wire.ServiceFlag).
	HasFiltering bool
}

// ConsensusDeployment defines details related to a specific consensus rule
// change that is voted in.  This is part of BIP0009.
type ConsensusDeployment struct {
	// BitNumber defines the specific bit number within the block version
	// this particular soft-fork deployment refers to.
	BitNumber uint8

	// StartTime is the median block time after which voting on the
	// deployment starts.
	StartTime uint64

	// ExpireTime is the median block time after which the attempted
	// deployment expires.
	ExpireTime uint64
}

const (
	// DeploymentTestDummy defines the rule change deployment ID for testing
	// purposes.
	DeploymentTestDummy = iota

	// DeploymentCSV defines the rule change deployment ID for the CSV
	// soft-fork package. The CSV package includes the deployment of BIPS
	// 68, 112, and 113.
	DeploymentCSV

	// NOTE: DefinedDeployments must always come last since it is used to
	// determine how many defined deployments there currently are.

	// DefinedDeployments is the number of currently defined deployments.
	DefinedDeployments
)

// Params defines the network parameters for a Bitcoin network.
type Params struct {
	// Name defines a human-readable identifier for the network.
	Name string

	// Net defines the magic bytes used to identify the network.
	Net wire.BitcoinNet

	// TopicPrefix defines the prefix used for blockchain topics in libP2P.
	TopicPrefix string

	// DefaultPort defines the default peer-to-peer port for the network.
	DefaultPort string

	// DNSSeeds defines a list of DNS seeds for the network that are used
	// as one method to discover peers.
	DNSSeeds []DNSSeed

	// GenesisBlock defines the first block of the chain.
	GenesisBlock *wire.MsgBlock

	// GenesisHash is the starting block hash.
	GenesisHash *chainhash.Hash

	// PowLimit defines the highest allowed proof of work value for a block
	// as a uint256.
	PowLimit *big.Int

	// PowLimitBits defines the highest allowed proof of work value for a
	// block in compact form.
	PowLimitBits uint32

	// These fields define the block heights at which the specified softfork
	// BIP became active.
	BIP0034Height int32
	BIP0065Height int32
	BIP0066Height int32
	// Block height at which CSV (BIP68, BIP112 and BIP113) becomes active
	CSVHeight uint32

	// The following are the heights at which the Bitcoin-specific forks
	// became active.
	UahfForkHeight          uint32 // August 1, 2017, hard fork
	DaaForkHeight           uint32 // November 13, 2017 hard fork
	GenesisActivationHeight uint32 // Genesis activation height

	// CoinbaseMaturity is the number of blocks required before newly mined
	// coins (coinbase transactions) can be spent.
	CoinbaseMaturity uint16

	// MaxCoinbaseScriptSigSize is the maximum size of the scriptSig in bytes for the coinbase transaction.
	MaxCoinbaseScriptSigSize uint32

	// SubsidyReductionInterval is the interval of blocks before the subsidy
	// is reduced.
	SubsidyReductionInterval uint32

	// TargetTimespan is the desired amount of time that should elapse
	// before the block difficulty requirement is examined to determine how
	// it should be changed to maintain the desired block
	// generation rate.
	// TargetTimespan time.Duration

	// TargetTimePerBlock is the desired amount of time to generate each
	// block.
	TargetTimePerBlock time.Duration

	// RetargetAdjustmentFactor is the adjustment factor used to limit
	// the minimum and maximum amount of adjustment that can occur between
	// difficulty retargets.
	RetargetAdjustmentFactor int64

	// ReduceMinDifficulty defines whether the network should reduce the
	// minimum required difficulty after a long enough period of time has
	// passed without finding a block.  This is really only useful for test
	// networks and should not be set on a main network.
	ReduceMinDifficulty bool

	// NoDifficultyAdjustment defines whether the network should skip the
	// normal difficulty adjustment and keep the current difficulty.
	NoDifficultyAdjustment bool

	// MinDiffReductionTime is the amount of time after which the minimum
	// required difficulty should be reduced when a block hasn't been found.
	//
	// NOTE: This only applies if ReduceMinDifficulty is true.
	MinDiffReductionTime time.Duration

	// GenerateSupported specifies whether CPU mining is allowed.
	GenerateSupported bool

	// Checkpoints ordered from oldest to newest.
	Checkpoints []Checkpoint

	// These fields are related to voting on consensus rule changes as
	// defined by BIP0009.
	//
	// RuleChangeActivationThreshold is the number of blocks in a threshold
	// state retarget window for which a positive vote for a rule change
	// must be cast to lock in a rule change. It should typically
	// be 95% for the main network and 75% for test networks.
	//
	// MinerConfirmationWindow is the number of blocks in each threshold
	// state retarget window.
	//
	// Deployments define the specific consensus rule changes to be voted
	// on.
	RuleChangeActivationThreshold uint32
	MinerConfirmationWindow       uint32
	Deployments                   [DefinedDeployments]ConsensusDeployment

	// Mempool parameters
	RelayNonStdTxs bool

	// The prefix used for the cashaddress. This is different for each network.
	CashAddressPrefix string

	// Address encoding magics
	LegacyPubKeyHashAddrID byte // First byte of a P2PKH address
	LegacyScriptHashAddrID byte // First byte of a P2SH address
	PrivateKeyID           byte // First byte of a WIF private key

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID [4]byte
	HDPublicKeyID  [4]byte

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType uint32
}

// MainNetParams defines the network parameters for the main Bitcoin network.
var MainNetParams = Params{
	Name:        "mainnet",
	Net:         wire.MainNet,
	TopicPrefix: "bitcoin/mainnet",
	DefaultPort: "8333",
	DNSSeeds: []DNSSeed{
		{"seed.bitcoinsv.io", true},
	},

	// Chain parameters
	GenesisBlock:  &genesisBlock,
	GenesisHash:   &genesisHash,
	PowLimit:      mainPowLimit,
	PowLimitBits:  0x1d00ffff,
	BIP0034Height: 227931, // 000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8
	BIP0065Height: 388381, // 000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0
	BIP0066Height: 363725, // 00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931
	CSVHeight:     419328, // 000000000000000004a1b34462cb8aeebd5799177f7a29cf28f2d1961716b5b5

	// August 1, 2017, hard fork
	UahfForkHeight: 478558, // 0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43

	// November 13, 2017, hard fork
	DaaForkHeight: 504031, // 0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c

	GenesisActivationHeight:  620538,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         100,
	SubsidyReductionInterval: 210000,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      false,
	NoDifficultyAdjustment:   false,
	MinDiffReductionTime:     0,
	GenerateSupported:        false,

	// Checkpoints ordered from oldest to newest.
	Checkpoints: []Checkpoint{
		{11111, newHashFromStr("0000000069e244f73d78e8fd29ba2fd2ed618bd6fa2ee92559f542fdb26e7c1d")},
		{33333, newHashFromStr("000000002dd5588a74784eaa7ab0507a18ad16a236e7b1ce69f00d7ddfb5d0a6")},
		{74000, newHashFromStr("0000000000573993a3c9e41ce34471c079dcf5f52a0e824a81e7f953b8661a20")},
		{105000, newHashFromStr("00000000000291ce28027faea320c8d2b054b2e0fe44a773f3eefb151d6bdc97")},
		{134444, newHashFromStr("00000000000005b12ffd4cd315cd34ffd4a594f430ac814c91184a0d42d2b0fe")},
		{168000, newHashFromStr("000000000000099e61ea72015e79632f216fe6cb33d7899acb35b75c8303b763")},
		{193000, newHashFromStr("000000000000059f452a5f7340de6682a977387c17010ff6e6c3bd83ca8b1317")},
		{210000, newHashFromStr("000000000000048b95347e83192f69cf0366076336c639f9b7228e9ba171342e")},
		{216116, newHashFromStr("00000000000001b4f4b433e81ee46494af945cf96014816a4e2370f11b23df4e")},
		{225430, newHashFromStr("00000000000001c108384350f74090433e7fcf79a606b8e797f065b130575932")},
		{250000, newHashFromStr("000000000000003887df1f29024b06fc2200b55f8af8f35453d7be294df2d214")},
		{267300, newHashFromStr("000000000000000a83fbd660e918f218bf37edd92b748ad940483c7c116179ac")},
		{279000, newHashFromStr("0000000000000001ae8c72a0b0c301f67e3afca10e819efa9041e458e9bd7e40")},
		{300255, newHashFromStr("0000000000000000162804527c6e9b9f0563a280525f9d08c12041def0a0f3b2")},
		{319400, newHashFromStr("000000000000000021c6052e9becade189495d1c539aa37c58917305fd15f13b")},
		{343185, newHashFromStr("0000000000000000072b8bf361d01a6ba7d445dd024203fafc78768ed4368554")},
		{352940, newHashFromStr("000000000000000010755df42dba556bb72be6a32f3ce0b6941ce4430152c9ff")},
		{382320, newHashFromStr("00000000000000000a8dc6ed5b133d0eb2fd6af56203e4159789b092defd8ab2")},
		{400000, newHashFromStr("000000000000000004ec466ce4732fe6f1ed1cddc2ed4b328fff5224276e3f6f")},
		{430000, newHashFromStr("000000000000000001868b2bb3a285f3cc6b33ea234eb70facf4dcdf22186b87")},
		{470000, newHashFromStr("0000000000000000006c539c722e280a0769abd510af0073430159d71e6d7589")},
		{510000, newHashFromStr("00000000000000000367922b6457e21d591ef86b360d78a598b14c2f1f6b0e04")},
		{552979, newHashFromStr("0000000000000000015648768ac1b788a83187d706f858919fcc5c096b76fbf2")},
		{556767, newHashFromStr("000000000000000001d956714215d96ffc00e0afda4cd0a96c96f8d802b1662b")},
		// checkpoints added for Teranode - this chunks up the initial sync
		{600000, newHashFromStr("00000000000000000866448ef293f900812d4af8e08cbe7ef62888eee9d29c4c")},
		{650000, newHashFromStr("00000000000000000310c17bbb4f3f8e5371a41ec2cee36a39876042019b725b")},
		{700000, newHashFromStr("00000000000000000e155235fd83a8757c44c6299e63104fb12632368f3f0cc9")},
		{750000, newHashFromStr("000000000000000006296f1e5437dd6c01b9b5471691a89a9c7d8e9f06920da5")},
		{800000, newHashFromStr("000000000000000000ad9056924410005d91b57f100bce345944e5caf56e8565")},
		{850000, newHashFromStr("0000000000000000039302a65227ab75fd93904ebe2e62421d1c66b15808b23b")},
		{868500, newHashFromStr("00000000000000000a4c8747ee369c2f4645cf7b55db534851fdc1a040f74de4")},
	},

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 1916, // 95% of MinerConfirmationWindow
	MinerConfirmationWindow:       2016, //
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  1199145601, // January 1, 2008, UTC
			ExpireTime: 1230767999, // December 31, 2008, UTC
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  1462060800, // May 1st, 2016
			ExpireTime: 1493596800, // May 1st, 2017
		},
	},

	// Mempool parameters
	RelayNonStdTxs: false,

	// The prefix for the cashaddress
	CashAddressPrefix: "bitcoincash", // always bitcoincash for mainnet

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x00, // starts with 1
	LegacyScriptHashAddrID: 0x05, // starts with 3
	PrivateKeyID:           0x80, // starts with 5 (uncompressed) or K (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x88, 0xad, 0xe4}, // starts with xprv
	HDPublicKeyID:  [4]byte{0x04, 0x88, 0xb2, 0x1e}, // starts with xpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 145,
}

// StnParams defines the network parameters for the scaling test network.
var StnParams = Params{
	Name:        "stn",
	Net:         wire.STN,
	TopicPrefix: "bitcoin/stn",
	DefaultPort: "9333",
	DNSSeeds:    []DNSSeed{},

	// Chain parameters
	GenesisBlock:             &stnGenesisBlock,
	GenesisHash:              &stnGenesisHash,
	PowLimit:                 stnPowLimit,
	PowLimitBits:             0x207fffff,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         100,
	BIP0034Height:            100000000, // Not active - Permit ver 1 blocks
	BIP0065Height:            1351,      // Used by regression tests
	BIP0066Height:            1251,      // Used by regression tests
	CSVHeight:                0,         // Always active on stn

	// August 1, 2017, hard fork
	UahfForkHeight: 15,

	// November 13, 2017, hard fork
	DaaForkHeight: 2200, // must be > 2016

	GenesisActivationHeight: 100,

	SubsidyReductionInterval: 210000,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      false,
	NoDifficultyAdjustment:   false,
	MinDiffReductionTime:     time.Minute * 20, // TargetTimePerBlock * 2
	GenerateSupported:        false,

	// Checkpoints ordered from oldest to newest.
	Checkpoints: nil,

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 108, // 75% of MinerConfirmationWindow
	MinerConfirmationWindow:       144,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
	},

	// Mempool parameters
	RelayNonStdTxs: true,

	// The prefix for the cashaddress
	CashAddressPrefix: "", // don't think this is needed

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x6f, // starts with m or n
	LegacyScriptHashAddrID: 0xc4, // starts with 2
	PrivateKeyID:           0xef, // starts with 9 (uncompressed) or c (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x35, 0x83, 0x94}, // starts with tprv
	HDPublicKeyID:  [4]byte{0x04, 0x35, 0x87, 0xcf}, // starts with tpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 1, // all coins use 1
}

// RegressionNetParams defines the network parameters for the regression test
// Bitcoin network.  Not to be confused with the test Bitcoin network (version
// 3), this network is sometimes simply called "testnet".
var RegressionNetParams = Params{
	Name:        "regtest",
	Net:         wire.RegTestNet,
	TopicPrefix: "bitcoin/regtest",
	DefaultPort: "18444",
	DNSSeeds:    []DNSSeed{},

	// Chain parameters
	GenesisBlock:             &regTestGenesisBlock,
	GenesisHash:              newHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"),
	PowLimit:                 regressionPowLimit,
	PowLimitBits:             0x207fffff,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         100,
	BIP0034Height:            100000000, // Not active - Permit ver 1 blocks
	BIP0065Height:            1351,      // Used by regression tests
	BIP0066Height:            1251,      // Used by regression tests
	CSVHeight:                576,       // Used by regression tests

	// UAHF is always enabled on regtest.
	UahfForkHeight: 0, // August 1, 2017, hard fork

	// November 13, 2017, hard fork is always on regtest.
	DaaForkHeight: 0,

	GenesisActivationHeight: 10000,

	SubsidyReductionInterval: 150,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      true,
	NoDifficultyAdjustment:   true,
	MinDiffReductionTime:     time.Minute * 20, // TargetTimePerBlock * 2
	GenerateSupported:        true,

	// Checkpoints ordered from oldest to newest.
	Checkpoints: nil,

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 108, // 75% of MinerConfirmationWindow
	MinerConfirmationWindow:       144,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
	},

	// Mempool parameters
	RelayNonStdTxs: true,

	// The prefix for the cashaddress
	CashAddressPrefix: "bsvreg", // always bsvreg for reg testnet

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x6f, // starts with m or n
	LegacyScriptHashAddrID: 0xc4, // starts with 2
	PrivateKeyID:           0xef, // starts with 9 (uncompressed) or c (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x35, 0x83, 0x94}, // starts with tprv
	HDPublicKeyID:  [4]byte{0x04, 0x35, 0x87, 0xcf}, // starts with tpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 1, // all coins use 1
}

// TestNetParams defines the network parameters for the test Bitcoin network
// (version 3).  Not to be confused with the regression test network, this
// network is sometimes simply called "testnet".
var TestNetParams = Params{
	Name:        "testnet",
	Net:         wire.TestNet,
	TopicPrefix: "bitcoin/testnet",
	DefaultPort: "18333",
	DNSSeeds: []DNSSeed{
		{"testnet-seed.bitcoinsv.io", true},
	},

	// Chain parameters
	GenesisBlock:  &testNetGenesisBlock,
	GenesisHash:   &testNetGenesisHash,
	PowLimit:      testNetPowLimit,
	PowLimitBits:  0x1d00ffff,
	BIP0034Height: 21111,  // 0000000023b3a96d3484e5abb3755c413e7d41500f8e2a5c3f0dd01299cd8ef8
	BIP0065Height: 581885, // 00000000007f6655f22f98e72ed80d8b06dc761d5da09df0fa1dc4be4f861eb6
	BIP0066Height: 330776, // 000000002104c8c45e99a8853285a3b592602a3ccde2b832481da85e9e4ba182
	CSVHeight:     770112, // 00000000025e930139bac5c6c31a403776da130831ab85be56578f3fa75369bb

	// August 1, 2017, hard fork
	UahfForkHeight: 1155875, // 00000000f17c850672894b9a75b63a1e72830bbd5f4c8889b5c1a80e7faef138

	// November 13, 2017, hard fork
	DaaForkHeight: 1188697, // 0000000000170ed0918077bde7b4d36cc4c91be69fa09211f748240dabe047fb

	GenesisActivationHeight:  1344302,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         100,

	SubsidyReductionInterval: 210000,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      true,
	NoDifficultyAdjustment:   false,
	MinDiffReductionTime:     time.Minute * 20, // TargetTimePerBlock * 2
	GenerateSupported:        false,

	// Checkpoints ordered from oldest to newest.
	Checkpoints: []Checkpoint{
		{546, newHashFromStr("000000002a936ca763904c3c35fce2f3556c559c0214345d31b1bcebf76acb70")},
		{100000, newHashFromStr("00000000009e2958c15ff9290d571bf9459e93b19765c6801ddeccadbb160a1e")},
		{200000, newHashFromStr("0000000000287bffd321963ef05feab753ebe274e1d78b2fd4e2bfe9ad3aa6f2")},
		{300001, newHashFromStr("0000000000004829474748f3d1bc8fcf893c88be255e6d7f571c548aff57abf4")},
		{400002, newHashFromStr("0000000005e2c73b8ecb82ae2dbc2e8274614ebad7172b53528aba7501f5a089")},
		{500011, newHashFromStr("00000000000929f63977fbac92ff570a9bd9e7715401ee96f2848f7b07750b02")},
		{600002, newHashFromStr("000000000001f471389afd6ee94dcace5ccc44adc18e8bff402443f034b07240")},
		{700000, newHashFromStr("000000000000406178b12a4dea3b27e13b3c4fe4510994fd667d7c1e6a3f4dc1")},
		{800010, newHashFromStr("000000000017ed35296433190b6829db01e657d80631d43f5983fa403bfdb4c1")},
		{900000, newHashFromStr("0000000000356f8d8924556e765b7a94aaebc6b5c8685dcfa2b1ee8b41acd89b")},
		{1000007, newHashFromStr("00000000001ccb893d8a1f25b70ad173ce955e5f50124261bbbc50379a612ddf")},
		{1100000, newHashFromStr("00000000001c2fb9880485b1f3d7b0ffa9fabdfd0cf16e29b122bb6275c73db0")},
		{1200000, newHashFromStr("00000000d91bdbb5394bcf457c0f0b7a7e43eb978e2d881b6c2a4c2756abc558")},
		{1300000, newHashFromStr("00000000000000f7569d4d0af19d8d0b59bb0b1a989caf0f552afb5c00d38fbf")},
		{1400000, newHashFromStr("000000000000008f84faa5afa3e30bce81599108f932eabdf9ee3d39bb225e5b")},
		{1500000, newHashFromStr("00000000000005a00d805e3555e53f18c6276cb5ddc90a3ceeaeaf03bb2fdbea")},
		{1600000, newHashFromStr("000000000000133137efc60aab38163c0d032d651826ccbda90b169f3bcec6dd")},
	},

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 1512, // 75% of MinerConfirmationWindow
	MinerConfirmationWindow:       2016,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  1199145601, // January 1, 2008, UTC
			ExpireTime: 1230767999, // December 31, 2008, UTC
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  1456790400, // March 1st, 2016
			ExpireTime: 1493596800, // May 1st, 2017
		},
	},

	// Mempool parameters
	RelayNonStdTxs: true,

	// The prefix for the cashaddress
	CashAddressPrefix: "bsvtest", // always bsvtest for testnet

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x6f, // starts with m or n
	LegacyScriptHashAddrID: 0xc4, // starts with 2
	PrivateKeyID:           0xef, // starts with 9 (uncompressed) or c (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x35, 0x83, 0x94}, // starts with tprv
	HDPublicKeyID:  [4]byte{0x04, 0x35, 0x87, 0xcf}, // starts with tpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 1, // all coins use 1
}

// TeraTestNetParams defines the network parameters for the tera test network
var TeraTestNetParams = Params{
	Name:        "teratestnet",
	Net:         wire.TeraTestNet,
	TopicPrefix: "bitcoin/teratestnet",
	DefaultPort: "18333",

	// Chain parameters
	GenesisBlock: &testNetGenesisBlock,
	GenesisHash:  &testNetGenesisHash,
	PowLimit:     testNetPowLimit,
	PowLimitBits: 0x207fffff, // very easy pow limit

	BIP0034Height: 100000000, // Not active - Permit ver 1 blocks
	BIP0065Height: 1351,      // Used by regression tests
	BIP0066Height: 1251,      // Used by regression tests
	CSVHeight:     0,

	UahfForkHeight: 0, // always enabled

	DaaForkHeight:            0, // always enabled
	GenesisActivationHeight:  1,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         10, // coinbase matures after 10 confirmations

	SubsidyReductionInterval: 210000,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      true,
	NoDifficultyAdjustment:   false,
	MinDiffReductionTime:     time.Minute * 20, // TargetTimePerBlock * 2
	GenerateSupported:        false,

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 1512, // 75% of MinerConfirmationWindow
	MinerConfirmationWindow:       2016,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
	},

	// Mempool parameters
	RelayNonStdTxs: true,

	// The prefix for the cashaddress
	CashAddressPrefix: "bsvtest", // always bsvtest for testnet

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x6f, // starts with m or n
	LegacyScriptHashAddrID: 0xc4, // starts with 2
	PrivateKeyID:           0xef, // starts with 9 (uncompressed) or c (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x35, 0x83, 0x94}, // starts with tprv
	HDPublicKeyID:  [4]byte{0x04, 0x35, 0x87, 0xcf}, // starts with tpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 1, // all coins use 1
}

// TeraScalingTestNetParams defines the network parameters for the teranode scaling test network
var TeraScalingTestNetParams = Params{
	Name:        "tstn",
	Net:         wire.TeraScalingTestNet,
	TopicPrefix: "bitcoin/tstn",
	DefaultPort: "18333",

	// Chain parameters
	GenesisBlock: &testNetGenesisBlock,
	GenesisHash:  &testNetGenesisHash,
	PowLimit:     testNetPowLimit,
	PowLimitBits: 0x207fffff, // very easy pow limit

	BIP0034Height: 100000000, // Not active - Permit ver 1 blocks
	BIP0065Height: 1351,      // Used by regression tests
	BIP0066Height: 1251,      // Used by regression tests
	CSVHeight:     0,

	UahfForkHeight: 0, // always enabled

	DaaForkHeight:            0, // always enabled
	GenesisActivationHeight:  1,
	MaxCoinbaseScriptSigSize: 100,
	CoinbaseMaturity:         10, // coinbase matures after 10 confirmations

	SubsidyReductionInterval: 210000,
	TargetTimePerBlock:       time.Minute * 10, // 10 minutes
	RetargetAdjustmentFactor: 4,                // 25% less, 400% more
	ReduceMinDifficulty:      true,
	NoDifficultyAdjustment:   false,
	MinDiffReductionTime:     time.Minute * 20, // TargetTimePerBlock * 2
	GenerateSupported:        false,

	// Consensus rule change deployments.
	//
	// The miner confirmation window is defined as:
	//   target proof of work timespan / target proof of work spacing
	RuleChangeActivationThreshold: 1512, // 75% of MinerConfirmationWindow
	MinerConfirmationWindow:       2016,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for votes
			ExpireTime: math.MaxInt64, // Never expires
		},
	},

	// Mempool parameters
	RelayNonStdTxs: true,

	// The prefix for the cashaddress
	CashAddressPrefix: "bsvtest", // always bsvtest for testnet

	// Address encoding magics
	LegacyPubKeyHashAddrID: 0x6f, // starts with m or n
	LegacyScriptHashAddrID: 0xc4, // starts with 2
	PrivateKeyID:           0xef, // starts with 9 (uncompressed) or c (compressed)

	// BIP32 hierarchical deterministic extended key magics
	HDPrivateKeyID: [4]byte{0x04, 0x35, 0x83, 0x94}, // starts with tprv
	HDPublicKeyID:  [4]byte{0x04, 0x35, 0x87, 0xcf}, // starts with tpub

	// BIP44 coin type used in the hierarchical deterministic path for
	// address generation.
	HDCoinType: 1, // all coins use 1
}

var (
	// ErrDuplicateNet describes an error where the parameters for a Bitcoin
	// network could not be set due to the network already being a standard
	// network or previously-registered into this package.
	ErrDuplicateNet = fmt.Errorf("duplicate Bitcoin network")

	// ErrUnknownHDKeyID describes an error where the provided id which
	// is intended to identify the network for a hierarchical deterministic
	// private extended key is not registered.
	ErrUnknownHDKeyID = fmt.Errorf("unknown hd private extended key bytes")
)

var (
	registeredNets      = make(map[wire.BitcoinNet]struct{})
	pubKeyHashAddrIDs   = make(map[byte]struct{})
	scriptHashAddrIDs   = make(map[byte]struct{})
	cashAddressPrefixes = make(map[string]struct{})
	hdPrivToPubKeyIDs   = make(map[[4]byte][]byte)
)

// String returns the hostname of the DNS seed in human-readable form.
func (d DNSSeed) String() string {
	return d.Host
}

// Register registers the passed network parameters for a Bitcoin network.  It
func Register(params *Params) error {
	if _, ok := registeredNets[params.Net]; ok {
		return ErrDuplicateNet
	}

	registeredNets[params.Net] = struct{}{}
	pubKeyHashAddrIDs[params.LegacyPubKeyHashAddrID] = struct{}{}
	scriptHashAddrIDs[params.LegacyScriptHashAddrID] = struct{}{}
	hdPrivToPubKeyIDs[params.HDPrivateKeyID] = params.HDPublicKeyID[:]

	// A valid cashaddress prefix for the given net followed by ':'.
	cashAddressPrefixes[params.CashAddressPrefix+":"] = struct{}{}

	return nil
}

// mustRegister performs the same function as Register except it panics if there
// is an error.  This should only be called from package init functions.
func mustRegister(params *Params) {
	if err := Register(params); err != nil {
		panic("failed to register network: " + err.Error())
	}
}

// IsPubKeyHashAddrID returns whether the passed id is found
func IsPubKeyHashAddrID(id byte) bool {
	_, ok := pubKeyHashAddrIDs[id]
	return ok
}

// IsScriptHashAddrID returns whether the passed id is found
func IsScriptHashAddrID(id byte) bool {
	_, ok := scriptHashAddrIDs[id]
	return ok
}

// IsCashAddressPrefix returns whether the passed prefix is a valid cashaddress
func IsCashAddressPrefix(prefix string) bool {
	prefix = strings.ToLower(prefix)
	_, ok := cashAddressPrefixes[prefix]

	return ok
}

// HDPrivateKeyToPublicKeyID converts the passed hierarchical deterministic key to a public key id.
func HDPrivateKeyToPublicKeyID(id []byte) ([]byte, error) {
	if len(id) != 4 {
		return nil, ErrUnknownHDKeyID
	}

	var key [4]byte

	copy(key[:], id)

	pubBytes, ok := hdPrivToPubKeyIDs[key]
	if !ok {
		return nil, ErrUnknownHDKeyID
	}

	return pubBytes, nil
}

// newHashFromStr converts the passed big-endian hex string into a
// chainhash.Hash.  It only differs from the one available in chainhash in that
// it panics on an error since it will only (and must only) be called with
// hard-coded, and therefore known good, hashes.
func newHashFromStr(hexStr string) *chainhash.Hash {
	hash, err := chainhash.NewHashFromStr(hexStr)
	if err != nil {
		panic(err)
	}

	return hash
}

// GetChainParams returns the chain parameters for the specified network.
func GetChainParams(network string) (*Params, error) {
	switch network {
	case "mainnet":
		return &MainNetParams, nil
	case "testnet":
		return &TestNetParams, nil
	case "regtest":
		return &RegressionNetParams, nil
	case "stn":
		return &StnParams, nil
	case "teratestnet":
		return &TeraTestNetParams, nil
	case "tstn":
		return &TeraScalingTestNetParams, nil
	default:
		return nil, fmt.Errorf("unknown network %s", network)
	}
}

// GetChainParamsFromConfig retrieves the chain parameters from the configuration
func GetChainParamsFromConfig() *Params {
	network, _ := gocore.Config().Get("network", "mainnet")
	chainParams, _ := GetChainParams(network)

	return chainParams
}

// init registers all default networks when the package is initialized.
func init() {
	// Register all default networks when the package is initialized.
	mustRegister(&MainNetParams)
	mustRegister(&TestNetParams)
	mustRegister(&RegressionNetParams)
	mustRegister(&StnParams)
	mustRegister(&TeraTestNetParams)
	mustRegister(&TeraScalingTestNetParams)
}
