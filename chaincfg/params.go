// Copyright (c) 2014-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package chaincfg

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
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
	// can have for the regression test network.  It is the value 2^255 - 1.
	regressionPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 255), bigOne)

	// testNet3PowLimit is the highest proof of work value a Bitcoin block
	// can have for the test network (version 3).  It is the value
	// 2^224 - 1.
	testNet3PowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 225), bigOne)
	// TODO: change this back
	// testNet3PowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)

	// simNetPowLimit is the highest proof of work value a Bitcoin block
	// can have for the simulation test network.  It is the value 2^255 - 1.
	simNetPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 255), bigOne)

	// stnPowLimit is the highest proof of work value a Bitcoin block can
	// have for the scaling test network. It is the value 2^224 - 1.
	stnPowLimit = new(big.Int).Sub(new(big.Int).Lsh(bigOne, 224), bigOne)
)

// Checkpoint identifies a known good point in the block chain.  Using
// checkpoints allows a few optimizations for old blocks during initial download
// and also prevents forks from old blocks.
//
// Each checkpoint is selected based upon several factors.  See the
// documentation for blockchain.IsCheckpointCandidate for details on the
// selection criteria.
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

// Constants that define the deployment offset in the deployments field of the
// parameters for each deployment.  This is useful to be able to get the details
// of a specific deployment by name.
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

// Params defines a Bitcoin network by its parameters.  These parameters may be
// used by Bitcoin applications to differentiate networks as well as addresses
// and keys for one network from those intended for use on another network.
type Params struct {
	// Name defines a human-readable identifier for the network.
	Name string

	// Net defines the magic bytes used to identify the network.
	Net wire.BitcoinNet

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

	// The following are the heights at which the Bitcoin specific forks
	// became active.
	UahfForkHeight int32 // August 1, 2017 hard fork
	DaaForkHeight  int32 // November 13, 2017 hard fork

	// Planned hardforks
	GreatWallActivationTime uint64 // May 15, 2019 hard fork

	// CoinbaseMaturity is the number of blocks required before newly mined
	// coins (coinbase transactions) can be spent.
	CoinbaseMaturity uint16

	// SubsidyReductionInterval is the interval of blocks before the subsidy
	// is reduced.
	SubsidyReductionInterval int32

	// TargetTimespan is the desired amount of time that should elapse
	// before the block difficulty requirement is examined to determine how
	// it should be changed in order to maintain the desired block
	// generation rate.
	TargetTimespan time.Duration

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

	// GenerateSupported specifies whether or not CPU mining is allowed.
	GenerateSupported bool

	// Checkpoints ordered from oldest to newest.
	Checkpoints []Checkpoint

	// These fields are related to voting on consensus rule changes as
	// defined by BIP0009.
	//
	// RuleChangeActivationThreshold is the number of blocks in a threshold
	// state retarget window for which a positive vote for a rule change
	// must be cast in order to lock in a rule change. It should typically
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
	DefaultPort: "8333",
	DNSSeeds: []DNSSeed{
		{"seed.bitcoinsv.io", true},
		{"btccash-seeder.bitcoinunlimited.info", true},
	},

	// Chain parameters
	GenesisBlock:  &genesisBlock,
	GenesisHash:   &genesisHash,
	PowLimit:      mainPowLimit,
	PowLimitBits:  0x1d00ffff,
	BIP0034Height: 227931, // 000000000000024b89b42a942fe0d9fea3bb44ab7bd1b19115dd6a759c0808b8
	BIP0065Height: 388381, // 000000000000000004c2b624ed5d7756c508d90fd0da2c7c679febfa6c4735f0
	BIP0066Height: 363725, // 00000000000000000379eaa19dce8c9b722d46ae6a57c2f1a988119488b50931

	UahfForkHeight: 478558, // 0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43
	DaaForkHeight:  504031, // 0000000000000000011ebf65b60d0a3de80b8175be709d653b4c1a1beeb6ab9c

	CoinbaseMaturity:         100,
	SubsidyReductionInterval: 210000,
	TargetTimespan:           time.Hour * 24 * 14, // 14 days
	TargetTimePerBlock:       time.Minute * 10,    // 10 minutes
	RetargetAdjustmentFactor: 4,                   // 25% less, 400% more
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
			StartTime:  1199145601, // January 1, 2008 UTC
			ExpireTime: 1230767999, // December 31, 2008 UTC
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
	DefaultPort: "9333",
	DNSSeeds:    []DNSSeed{},

	// Chain parameters
	GenesisBlock:     &stnGenesisBlock,
	GenesisHash:      &stnGenesisHash,
	PowLimit:         stnPowLimit,
	PowLimitBits:     0x207fffff,
	CoinbaseMaturity: 100,
	BIP0034Height:    100000000, // Not active - Permit ver 1 blocks
	BIP0065Height:    1351,      // Used by regression tests
	BIP0066Height:    1251,      // Used by regression tests

	UahfForkHeight: 0, // Always active on regtest
	DaaForkHeight:  0, // Always active on regtest

	SubsidyReductionInterval: 210000,
	TargetTimespan:           time.Hour * 24 * 14, // 14 days
	TargetTimePerBlock:       time.Minute * 10,    // 10 minutes
	RetargetAdjustmentFactor: 4,                   // 25% less, 400% more
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
	RuleChangeActivationThreshold: 108, // 75%  of MinerConfirmationWindow
	MinerConfirmationWindow:       144,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for vote
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for vote
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
	Net:         wire.TestNet,
	DefaultPort: "18444",
	DNSSeeds:    []DNSSeed{},

	// Chain parameters
	GenesisBlock:     &regTestGenesisBlock,
	GenesisHash:      &regTestGenesisHash,
	PowLimit:         regressionPowLimit,
	PowLimitBits:     0x207fffff,
	CoinbaseMaturity: 100,
	BIP0034Height:    100000000, // Not active - Permit ver 1 blocks
	BIP0065Height:    1351,      // Used by regression tests
	BIP0066Height:    1251,      // Used by regression tests

	UahfForkHeight: 15,   // August 1, 2017 hard fork
	DaaForkHeight:  2200, // must be > 2016 - see assert in pow.cpp:268

	SubsidyReductionInterval: 150,
	TargetTimespan:           time.Hour * 24 * 14, // 14 days
	TargetTimePerBlock:       time.Minute * 10,    // 10 minutes
	RetargetAdjustmentFactor: 4,                   // 25% less, 400% more
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
	RuleChangeActivationThreshold: 108, // 75%  of MinerConfirmationWindow
	MinerConfirmationWindow:       144,
	Deployments: [DefinedDeployments]ConsensusDeployment{
		DeploymentTestDummy: {
			BitNumber:  28,
			StartTime:  0,             // Always available for vote
			ExpireTime: math.MaxInt64, // Never expires
		},
		DeploymentCSV: {
			BitNumber:  0,
			StartTime:  0,             // Always available for vote
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

//         strNetworkID = "stn";

//         consensus.BIP34Height = 100000000;

//         base58Prefixes[PUBKEY_ADDRESS] = std::vector<uint8_t>(1, 111);
//         base58Prefixes[SCRIPT_ADDRESS] = std::vector<uint8_t>(1, 196);
//         base58Prefixes[SECRET_KEY] = std::vector<uint8_t>(1, 239);
//         base58Prefixes[EXT_PUBLIC_KEY] = {0x04, 0x35, 0x87, 0xCF};
//         base58Prefixes[EXT_SECRET_KEY] = {0x04, 0x35, 0x83, 0x94};

//         // August 1, 2017 hard fork
//         consensus.uahfHeight = 15;

//         // November 13, 2017 hard fork
//         consensus.daaHeight = 2200;     // must be > 2016 - see assert in pow.cpp:268

//         std::vector<unsigned char> rawScript(ParseHex("76a914a123a6fdc265e1bbcf1123458891bd7af1a1b5d988ac"));
//         CScript outputScript(rawScript.begin(), rawScript.end());

//         genesis = CreateGenesisBlock(1296688602, 414098458, 0x1d00ffff, 1, 50 * COIN);
//         consensus.hashGenesisBlock = genesis.GetHash();
//         assert(consensus.hashGenesisBlock ==
//                uint256S("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526"
//                         "f8d77f4943"));

//         consensus.nSubsidyHalvingInterval = 210000;
//         consensus.BIP34Hash = uint256();
//         consensus.powLimit = uint256S("00000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
//         consensus.nPowTargetTimespan = 14 * 24 * 60 * 60; // two weeks
//         consensus.nPowTargetSpacing = 10 * 60;
//         // Do not allow min difficulty blocks after some time has elapsed
//         consensus.fPowAllowMinDifficultyBlocks = false;
//         consensus.fPowNoRetargeting = false;

//         // The best chain should have at least this much work.
//         consensus.nMinimumChainWork = uint256S("0x00");

//         // February 2020, Genesis Upgrade
//         consensus.genesisHeight = GENESIS_ACTIVATION_STN;

//         /**
//          * The message start string is designed to be unlikely to occur in
//          * normal data. The characters are rarely used upper ASCII, not valid as
//          * UTF-8, and produce a large 32-bit integer with any alignment.
//          */
//         diskMagic[0] = 0xfb;
//         diskMagic[1] = 0xce;
//         diskMagic[2] = 0xc4;
//         diskMagic[3] = 0xf9;
//         netMagic[0] = 0xfb;
//         netMagic[1] = 0xce;
//         netMagic[2] = 0xc4;
//         netMagic[3] = 0xf9;
//         nDefaultPort = 9333;
//         nPruneAfterHeight = 1000;

//         vFixedSeeds.clear();
//         vSeeds.clear();
//         vSeeds.push_back(CDNSSeedData("bitcoinsv.io", "stn-seed.bitcoinsv.io", true));
//         vSeeds.push_back(CDNSSeedData("bitcoinseed.directory",
//                                       "stn-seed.bitcoinseed.directory",
//                                       true));

//         vFixedSeeds = std::vector<SeedSpec6>();

//         fMiningRequiresPeers = true;
//         fDefaultConsistencyChecks = false;
//         fRequireStandard = false;
//         fMineBlocksOnDemand = false;

//         checkpointData = {  {
//                 {0, uint256S("000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943")},
//                 {1, uint256S("00000000e23f9436cc8a6d6aaaa515a7b84e7a1720fc9f92805c0007c77420c4")},
//                 {2, uint256S("0000000040f8f40b5111d037b8b7ff69130de676327bcbd76ca0e0498a06c44a")},
//                 {4, uint256S("00000000d33661d5a6906f84e3c64ea6101d144ec83760bcb4ba81edcb15e68d")},
//                 {5, uint256S("00000000e9222ebe623bf53f6ec774619703c113242327bdc24ac830787873d6")},
//                 {6, uint256S("00000000764a4ff15c2645e8ede0d0f2af169f7a517dd94a6778684ed85a51e4")},
//                 {7, uint256S("000000001f15fe3dac966c6bb873c63348ca3d877cd606759d26bd9ad41e5545")},
//                 {8, uint256S("0000000074230d332b2ed9d87af3ad817b6f2616c154372311c9b2e4f386c24c")},
//                 {9, uint256S("00000000ca21de811f04f5ec031aa3a102f8e27f2a436cde588786da1996ec9b")},
//                 {10, uint256S("0000000046ceee1b7d771594c6c75f11f14f96822fd520e86ec5c703ec231e87")}
//         }};

//         defaultBlockSizeParams = DefaultBlockSizeParams{
//             // activation time
//             STN_NEW_BLOCKSIZE_ACTIVATION_TIME,
//             // max block size
//             STN_DEFAULT_MAX_BLOCK_SIZE,
//             // max generated block size before activation
//             STN_DEFAULT_MAX_GENERATED_BLOCK_SIZE_BEFORE,
//             // max generated block size after activation
//             STN_DEFAULT_MAX_GENERATED_BLOCK_SIZE_AFTER
//         };

//         fTestBlockCandidateValidity = false;
//         fDisableBIP30Checks = false;
//         fCanDisableBIP30Checks = true;
//         fIsRegTest = false;

// TestNet3Params defines the network parameters for the test Bitcoin network
// (version 3).  Not to be confused with the regression test network, this
// network is sometimes simply called "testnet".
var TestNet3Params = Params{
	Name:        "testnet3",
	Net:         wire.TestNet3,
	DefaultPort: "18333",
	DNSSeeds: []DNSSeed{
		{"testnet-seed.bitcoinsv.io", true},
		{"testnet-btccash-seeder.bitcoinunlimited.info", true},
	},

	// Chain parameters
	GenesisBlock: &testNet3GenesisBlock,
	GenesisHash:  &testNet3GenesisHash,
	PowLimit:     testNet3PowLimit,
	PowLimitBits: 0x2000ffff,
	// PowLimitBits:  0x1d00ffff,
	BIP0034Height: 21111,  // 0000000023b3a96d3484e5abb3755c413e7d41500f8e2a5c3f0dd01299cd8ef8
	BIP0065Height: 581885, // 00000000007f6655f22f98e72ed80d8b06dc761d5da09df0fa1dc4be4f861eb6
	BIP0066Height: 330776, // 000000002104c8c45e99a8853285a3b592602a3ccde2b832481da85e9e4ba182

	UahfForkHeight: 1155875, // 00000000f17c850672894b9a75b63a1e72830bbd5f4c8889b5c1a80e7faef138
	DaaForkHeight:  1188697, // 0000000000170ed0918077bde7b4d36cc4c91be69fa09211f748240dabe047fb

	CoinbaseMaturity:         100,
	SubsidyReductionInterval: 210000,
	TargetTimespan:           time.Hour * 24 * 14, // 14 days
	// TODO: change this back to 10 mins
	TargetTimePerBlock:       time.Minute * 1, // 10 minutes
	RetargetAdjustmentFactor: 4,               // 25% less, 400% more
	ReduceMinDifficulty:      true,
	NoDifficultyAdjustment:   false,
	// TODO: change this back to 20 mins
	MinDiffReductionTime: time.Minute * 2, // TargetTimePerBlock * 2
	GenerateSupported:    false,

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
			StartTime:  1199145601, // January 1, 2008 UTC
			ExpireTime: 1230767999, // December 31, 2008 UTC
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

var (
	// ErrDuplicateNet describes an error where the parameters for a Bitcoin
	// network could not be set due to the network already being a standard
	// network or previously-registered into this package.
	ErrDuplicateNet = errors.New("duplicate Bitcoin network")

	// ErrUnknownHDKeyID describes an error where the provided id which
	// is intended to identify the network for a hierarchical deterministic
	// private extended key is not registered.
	ErrUnknownHDKeyID = errors.New("unknown hd private extended key bytes")
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

// Register registers the network parameters for a Bitcoin network.  This may
// error with ErrDuplicateNet if the network is already registered (either
// due to a previous Register call, or the network being one of the default
// networks).
//
// Network parameters should be registered into this package by a main package
// as early as possible.  Then, library packages may lookup networks or network
// parameters based on inputs and work regardless of the network being standard
// or not.
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

// IsPubKeyHashAddrID returns whether the id is an identifier known to prefix a
// pay-to-pubkey-hash address on any default or registered network.  This is
// used when decoding an address string into a specific address type.  It is up
// to the caller to check both this and IsScriptHashAddrID and decide whether an
// address is a pubkey hash address, script hash address, neither, or
// undeterminable (if both return true).
func IsPubKeyHashAddrID(id byte) bool {
	_, ok := pubKeyHashAddrIDs[id]
	return ok
}

// IsScriptHashAddrID returns whether the id is an identifier known to prefix a
// pay-to-script-hash address on any default or registered network.  This is
// used when decoding an address string into a specific address type.  It is up
// to the caller to check both this and IsPubKeyHashAddrID and decide whether an
// address is a pubkey hash address, script hash address, neither, or
// undeterminable (if both return true).
func IsScriptHashAddrID(id byte) bool {
	_, ok := scriptHashAddrIDs[id]
	return ok
}

// IsCashAddressPrefix returns whether the prefix is a known prefix for the
// cashaddress on any default or registered network.  This is used when decoding
// an address string into a specific address type.
func IsCashAddressPrefix(prefix string) bool {
	prefix = strings.ToLower(prefix)
	_, ok := cashAddressPrefixes[prefix]
	return ok
}

// HDPrivateKeyToPublicKeyID accepts a private hierarchical deterministic
// extended key id and returns the associated public key id.  When the provided
// id is not registered, the ErrUnknownHDKeyID error will be returned.
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
		// Ordinarily I don't like panics in library code since it
		// can take applications down without them having a chance to
		// recover which is extremely annoying, however an exception is
		// being made in this case because the only way this can panic
		// is if there is an error in the hard-coded hashes.  Thus it
		// will only ever potentially panic on init and therefore is
		// 100% predictable.
		panic(err)
	}
	return hash
}

func GetChainParams(network string) (*Params, error) {
	switch network {
	case "mainnet":
		return &MainNetParams, nil
	case "testnet":
		return &TestNet3Params, nil
	case "regtest":
		return &RegressionNetParams, nil
	case "stn":
		return &StnParams, nil
	default:
		return nil, errors.New(fmt.Sprintf("unknown network %s", network))
	}
}

func init() {
	// Register all default networks when the package is initialized.
	mustRegister(&MainNetParams)
	mustRegister(&TestNet3Params)
	mustRegister(&RegressionNetParams)
	mustRegister(&StnParams)
}

// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:59      | bchn  | ++++++GetNextWorkRequired++++++ bestBlockHeader.Hash: 00000000db8dcee5710fc1d960d3390e9e066ffe0b24a6bc9bdd8984c7cedc0a
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:163     | bchn  | firstSuitableBlock hash: 391b9ea96dedad4a1bd448aa7e22058e95e69301c913e9b532ef2abe79adcdfe
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:164     | bchn  | lastSuitableBlock hash: 006ad2bc13d081f41cc271c35d9a7059f346178248e9d5f4dbe4831bae40ff87
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:243     | bchn  | work: 4295036295
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:253     | bchn  | duration: 5192
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:256     | bchn  | projectedWork: 257702177700
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:259     | bchn  | pw: 49634471
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:262     | bchn  | e: 115792089237316195423570985008687907853269984665640564039457584007913129639936
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:265     | bchn  | nt: 115792089237316195423570985008687907853269984665640564039457584007913080005465
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:275     | bchn  | computeTarget - returning new target: 26959946667150639794667015087019630673637144422540572481103610249215
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Difficulty.go:178     | bchn  | CalculateNextWorkRequired: returning nBits: 1d00ffff
// 2024-08-30T11:32:35+01:00 | DEBUG | blockchain/Server.go:496         | bchn  | difficulty adjustment. Difficulty set to 1d00ffff
// 2024-08-30T11:32:35+01:00 | DEBUG | tracing/tracing.go:122           | prop  | [ProcessTransactionBatch] called for 1 transactions

// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:59      | bchn  | ++++++GetNextWorkRequired++++++ bestBlockHeader.Hash: 0093ee1cd9bb47028be9bf8d8b6cbd04576def7251f6d531aa2f543f0ed9ef52
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:163     | bchn  | firstSuitableBlock hash: 2d2822c2c86a723f8e66b669fca4de08d0e809ee23d1c5d30a575cc24628c246
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:164     | bchn  | lastSuitableBlock hash: 0088237f7cdddb305f8a9dcb7195cd3c2c55b9945eb1b60fa1c08c77cb1afcf1
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:243     | bchn  | work: 4295035536
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:253     | bchn  | duration: 5062
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:256     | bchn  | projectedWork: 257702132160
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:259     | bchn  | pw: 50909152
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:262     | bchn  | e: 115792089237316195423570985008687907853269984665640564039457584007913129639936
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:265     | bchn  | nt: 115792089237316195423570985008687907853269984665640564039457584007913078730784
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:275     | bchn  | computeTarget - returning new target: 26959946667150639794667015087019630673637144422540572481103610249215
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Difficulty.go:178     | bchn  | CalculateNextWorkRequired: returning nBits: 1d00ffff
// 2024-08-30T11:30:38+01:00 | DEBUG | blockchain/Server.go:496         | bchn  | difficulty adjustment. Difficulty set to 1d00ffff
