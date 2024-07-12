// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package legacy

import (
	"fmt"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/version"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/btcsuite/go-socks/socks"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	defaultDataDir                 = "data"
	defaultMaxPeers                = 125
	defaultMaxPeersPerIP           = 5
	defaultBanDuration             = time.Hour * 24
	defaultBanThreshold            = 100
	defaultConnectTimeout          = time.Second * 30
	defaultFreeTxRelayLimit        = 15.0
	defaultTrickleInterval         = peer.DefaultTrickleInterval
	defaultExcessiveBlockSize      = math.MaxUint64
	defaultBlockMinSize            = 0
	defaultBlockMaxSize            = defaultExcessiveBlockSize - 1000
	blockMaxSizeMin                = 1000
	defaultGenerate                = false
	defaultSigCacheMaxSize         = 100000
	defaultTxIndex                 = false
	defaultAddrIndex               = false
	defaultUtxoCacheMaxSizeMiB     = 450
	defaultMinSyncPeerNetworkSpeed = 51200
	defaultPruneDepth              = 4320
	defaultTargetOutboundPeers     = uint32(8)
	minPruneDepth                  = 288
	defaultMinRelayTxFee           = bsvutil.Amount(1)
)

type Config interface {
	Set(key string, value string) string
	Unset(key string) string
	Get(key string, defaultValue ...string) (string, bool)
	GetMulti(key string, sep string, defaultValue ...[]string) ([]string, bool)
	GetInt(key string, defaultValue ...int) (int, bool)
	GetBool(key string, defaultValue ...bool) bool
	GetDuration(key string, defaultValue ...time.Duration) (time.Duration, error, bool)
	GetURL(key string, defaultValue ...string) (*url.URL, error, bool)
	GetAll() map[string]string
}

// runServiceCommand is only set to a real function on Windows.  It is used
// to parse and execute service commands specified via the -s flag.
var runServiceCommand func(string) error

// minUint32 is a helper function to return the minimum of two uint32s.
// This avoids a math import and the need to cast to floats.
func minUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

// minUint64 is a helper function to return the minimum of two uint64s.
// This avoids a math import and the need to cast to floats.
func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// maxUint32 is a helper function to return the maximum of two uint32s.
// This avoids a math import and the need to cast to floats.
func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

// maxUint64 is a helper function to return the maximum of two uint64s.
// This avoids a math import and the need to cast to floats.
func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// config defines the configuration options for bsvd.
//
// See loadConfig for details on the configuration load process.
type config struct {
	ShowVersion             bool          `short:"V" long:"version" description:"Display version information and exit"`
	DataDir                 string        `short:"b" long:"datadir" description:"Directory to store data"`
	AddPeers                []string      `short:"a" long:"addpeer" description:"Add a peer to connect with at startup"`
	ConnectPeers            []string      `long:"connect" description:"Connect only to the specified peers at startup"`
	DisableListen           bool          `long:"nolisten" description:"Disable listening for incoming connections -- NOTE: Listening is automatically disabled if the --connect or --proxy options are used without also specifying listen interfaces via --listen"`
	Listeners               []string      `long:"listen" description:"Add an interface/port to listen for connections (default all interfaces port: 8333, testnet: 18333)"`
	MaxPeers                int           `long:"maxpeers" description:"Max number of inbound and outbound peers"`
	MaxPeersPerIP           int           `long:"maxpeersperip" description:"Max number of inbound and outbound peers per IP"`
	MinSyncPeerNetworkSpeed uint64        `long:"minsyncpeernetworkspeed" description:"Disconnect sync peers slower than this threshold in bytes/sec"`
	DisableBanning          bool          `long:"nobanning" description:"Disable banning of misbehaving peers"`
	BanDuration             time.Duration `long:"banduration" description:"How long to ban misbehaving peers.  Valid time units are {s, m, h}.  Minimum 1 second"`
	BanThreshold            uint32        `long:"banthreshold" description:"Maximum allowed ban score before disconnecting and banning misbehaving peers."`
	Whitelists              []string      `long:"whitelist" description:"Add an IP network or IP that will not be banned. (eg. 192.168.1.0/24 or ::1)"`
	DisableTLS              bool          `long:"notls" description:"Disable TLS for the RPC server -- NOTE: This is only allowed if the RPC server is bound to localhost"`
	DisableDNSSeed          bool          `long:"nodnsseed" description:"Disable DNS seeding for peers"`
	ExternalIPs             []string      `long:"externalip" description:"Add an ip to the list of local addresses we claim to listen on to peers"`
	Proxy                   string        `long:"proxy" description:"Connect via SOCKS5 proxy (eg. 127.0.0.1:9050)"`
	ProxyUser               string        `long:"proxyuser" description:"Username for proxy server"`
	ProxyPass               string        `long:"proxypass" default-mask:"-" description:"Password for proxy server"`
	TestNet3                bool          `long:"testnet" description:"Use the test network"`
	RegressionTest          bool          `long:"regtest" description:"Use the regression test network"`
	SimNet                  bool          `long:"simnet" description:"Use the simulation test network"`
	AddCheckpoints          []string      `long:"addcheckpoint" description:"Add a custom checkpoint.  Format: '<height>:<hash>'"`
	DisableCheckpoints      bool          `long:"nocheckpoints" description:"Disable built-in checkpoints.  Don't do this unless you know what you're doing."`
	Profile                 string        `long:"profile" description:"Enable HTTP profiling on given port -- NOTE port must be between 1024 and 65536"`
	CPUProfile              string        `long:"cpuprofile" description:"Write CPU profile to the specified file"`
	DebugLevel              string        `short:"d" long:"debuglevel" description:"Logging level for all subsystems {trace, debug, info, warn, error, critical} -- You may also specify <subsystem>=<level>,<subsystem2>=<level>,... to set the log level for individual subsystems -- Use show to list available subsystems"`
	Upnp                    bool          `long:"upnp" description:"Use UPnP to map our listening port outside of NAT"`
	ExcessiveBlockSize      uint64        `long:"excessiveblocksize" description:"The maximum size block (in bytes) this node will accept. Cannot be less than 32000000."`
	MinRelayTxFee           float64       `long:"minrelaytxfee" description:"The minimum transaction fee in BSV/kB to be considered a non-zero fee."`
	FreeTxRelayLimit        float64       `long:"limitfreerelay" description:"Limit relay of transactions with no transaction fee to the given amount in thousands of bytes per minute"`
	NoRelayPriority         bool          `long:"norelaypriority" description:"Do not require free or low-fee transactions to have high priority for relaying"`
	TrickleInterval         time.Duration `long:"trickleinterval" description:"Minimum time between attempts to send new inventory to a connected peer"`
	MaxOrphanTxs            int           `long:"maxorphantx" description:"Max number of orphan transactions to keep in memory"`
	Generate                bool          `long:"generate" description:"Generate (mine) bitcoins using the CPU"`
	MiningAddrs             []string      `long:"miningaddr" description:"Add the specified payment address to the list of addresses to use for generated blocks -- At least one address is required if the generate option is set"`
	BlockMinSize            uint64        `long:"blockminsize" description:"Mininum block size in bytes to be used when creating a block"`
	BlockMaxSize            uint64        `long:"blockmaxsize" description:"Maximum block size in bytes to be used when creating a block"`
	BlockPrioritySize       uint64        `long:"blockprioritysize" description:"Size in bytes for high-priority/low-fee transactions when creating a block"`
	UserAgentComments       []string      `long:"uacomment" description:"Comment to add to the user agent -- See BIP 14 for more information."`
	NoPeerBloomFilters      bool          `long:"nopeerbloomfilters" description:"Disable bloom filtering support"`
	NoCFilters              bool          `long:"nocfilters" description:"Disable committed filtering (CF) support"`
	SigCacheMaxSize         uint          `long:"sigcachemaxsize" description:"The maximum number of entries in the signature verification cache"`
	UtxoCacheMaxSizeMiB     uint          `long:"utxocachemaxsize" description:"The maximum size in MiB of the UTXO cache"`
	BlocksOnly              bool          `long:"blocksonly" description:"Do not accept transactions from remote peers."`
	TxIndex                 bool          `long:"txindex" description:"Maintain a full hash-based transaction index which makes all transactions available via the getrawtransaction RPC"`
	AddrIndex               bool          `long:"addrindex" description:"Maintain a full address-based transaction index which makes the searchrawtransactions RPC available"`
	RelayNonStd             bool          `long:"relaynonstd" description:"Relay non-standard transactions regardless of the default settings for the active network."`
	RejectNonStd            bool          `long:"rejectnonstd" description:"Reject non-standard transactions regardless of the default settings for the active network."`
	Prune                   bool          `long:"prune" description:"Delete historical blocks from the chain. A buffer of blocks will be retained in case of a reorg."`
	PruneDepth              uint32        `long:"prunedepth" description:"The number of blocks to retain when running in pruned mode. Cannot be less than 288."`
	TargetOutboundPeers     uint32        `long:"targetoutboundpeers" description:"number of outbound connections to maintain"`
	lookup                  func(string) ([]net.IP, error)
	dial                    func(string, string, time.Duration) (net.Conn, error)
	addCheckpoints          []chaincfg.Checkpoint
	miningAddrs             []bsvutil.Address
	minRelayTxFee           bsvutil.Amount
	whitelists              []*net.IPNet
}

// serviceOptions defines the configuration options for the daemon as a service on
// Windows.
type serviceOptions struct {
	ServiceCommand string `short:"s" long:"service" description:"Service command {install, remove, start, stop}"`
}

// removeDuplicateAddresses returns a new slice with all duplicate entries in
// addrs removed.
func removeDuplicateAddresses(addrs []string) []string {
	result := make([]string, 0, len(addrs))
	seen := map[string]struct{}{}
	for _, val := range addrs {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = struct{}{}
		}
	}
	return result
}

// normalizeAddress returns addr with the passed default port appended if
// there is not already a port specified.
func normalizeAddress(addr, defaultPort string) string {
	_, _, err := net.SplitHostPort(addr)
	if err != nil {
		return net.JoinHostPort(addr, defaultPort)
	}
	return addr
}

// normalizeAddresses returns a new slice with all the passed peer addresses
// normalized with the given default port, and all duplicates removed.
func normalizeAddresses(addrs []string, defaultPort string) []string {
	for i, addr := range addrs {
		addrs[i] = normalizeAddress(addr, defaultPort)
	}

	return removeDuplicateAddresses(addrs)
}

// newCheckpointFromStr parses checkpoints in the '<height>:<hash>' format.
func newCheckpointFromStr(checkpoint string) (chaincfg.Checkpoint, error) {
	parts := strings.Split(checkpoint, ":")
	if len(parts) != 2 {
		return chaincfg.Checkpoint{}, fmt.Errorf("unable to parse "+
			"checkpoint %q -- use the syntax <height>:<hash>",
			checkpoint)
	}

	height, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return chaincfg.Checkpoint{}, fmt.Errorf("unable to parse "+
			"checkpoint %q due to malformed height", checkpoint)
	}

	if len(parts[1]) == 0 {
		return chaincfg.Checkpoint{}, fmt.Errorf("unable to parse "+
			"checkpoint %q due to missing hash", checkpoint)
	}
	hash, err := chainhash.NewHashFromStr(parts[1])
	if err != nil {
		return chaincfg.Checkpoint{}, fmt.Errorf("unable to parse "+
			"checkpoint %q due to malformed hash", checkpoint)
	}

	return chaincfg.Checkpoint{
		Height: int32(height),
		Hash:   hash,
	}, nil
}

// parseCheckpoints checks the checkpoint strings for valid syntax
// ('<height>:<hash>') and parses them to chaincfg.Checkpoint instances.
func parseCheckpoints(checkpointStrings []string) ([]chaincfg.Checkpoint, error) {
	if len(checkpointStrings) == 0 {
		return nil, nil
	}
	checkpoints := make([]chaincfg.Checkpoint, len(checkpointStrings))
	for i, cpString := range checkpointStrings {
		checkpoint, err := newCheckpointFromStr(cpString)
		if err != nil {
			return nil, err
		}
		checkpoints[i] = checkpoint
	}
	return checkpoints, nil
}

// newConfigParser returns a new command line flags parser.
//func newConfigParser(cfg *config, so *serviceOptions, options flags.Options) *flags.Parser {
//	parser := flags.NewParser(cfg, options)
//	if runtime.GOOS == "windows" {
//		parser.AddGroup("Service Options", "Service Options", so)
//	}
//	return parser
//}

// loadConfig initializes and parses the config using a config file and command
// line options.
//
// The configuration proceeds as follows:
//  1. Start with a default config with sane settings
//  2. Pre-parse the command line to check for an alternative config file
//  3. Load configuration file overwriting defaults with any specified options
//  4. Parse CLI options and overwrite/add any specified options
//
// The above results in bsvd functioning properly without any config settings
// while still allowing the user to override settings with config files and
// command line options.  Command line options always take precedence.
func loadConfig(logger ulogger.Logger) (*config, []string, error) {
	// Default config.
	cfg := config{
		MaxPeers:                defaultMaxPeers,
		MaxPeersPerIP:           defaultMaxPeersPerIP,
		MinSyncPeerNetworkSpeed: defaultMinSyncPeerNetworkSpeed,
		BanDuration:             defaultBanDuration,
		BanThreshold:            defaultBanThreshold,
		DataDir:                 defaultDataDir,
		ExcessiveBlockSize:      defaultExcessiveBlockSize,
		MinRelayTxFee:           defaultMinRelayTxFee.ToBSV(),
		FreeTxRelayLimit:        defaultFreeTxRelayLimit,
		TrickleInterval:         defaultTrickleInterval,
		BlockMinSize:            defaultBlockMinSize,
		BlockMaxSize:            defaultBlockMaxSize,
		BlockPrioritySize:       uint64(50000), // TODO change this to something else
		SigCacheMaxSize:         defaultSigCacheMaxSize,
		UtxoCacheMaxSizeMiB:     defaultUtxoCacheMaxSizeMiB,
		Generate:                defaultGenerate,
		TxIndex:                 defaultTxIndex,
		AddrIndex:               defaultAddrIndex,
		PruneDepth:              defaultPruneDepth,
		TargetOutboundPeers:     defaultTargetOutboundPeers,
	}

	// Service options which are only added on Windows.
	serviceOpts := serviceOptions{}

	// Pre-parse the command line options to see if an alternative config
	// file or the version flag was specified.  Any errors aside from the
	// help message error can be ignored here since they will be caught by
	// the final parse below.
	preCfg := cfg

	// Show the version and exit if the version flag was specified.
	appName := filepath.Base(os.Args[0])
	appName = strings.TrimSuffix(appName, filepath.Ext(appName))
	usageMessage := fmt.Sprintf("Use %s -h to show usage", appName)
	if preCfg.ShowVersion {
		logger.Infof("app: %s, version: %s", appName, version.String())
		os.Exit(0)
	}

	// Perform service command and exit if specified.  Invalid service
	// commands show an appropriate error.  Only runs on Windows since
	// the runServiceCommand function will be nil when not on Windows.
	if serviceOpts.ServiceCommand != "" && runServiceCommand != nil {
		err := runServiceCommand(serviceOpts.ServiceCommand)
		if err != nil {
			logger.Errorf("%v", err)
		}
		os.Exit(0)
	}

	// Load additional config from file.
	//parser := newConfigParser(&cfg, &serviceOpts, flags.Default)
	//if !(preCfg.RegressionTest || preCfg.SimNet) || preCfg.ConfigFile !=
	//	defaultConfigFile {
	//
	//	err := flags.NewIniParser(parser).ParseFile(preCfg.ConfigFile)
	//	if err != nil {
	//		if _, ok := err.(*os.PathError); !ok {
	//			fmt.Fprintf(os.Stderr, "Error parsing config file: %v\n", err)
	//			logger.Errorf("%s", usageMessage)
	//			return nil, nil, err
	//		}
	//	}
	//}

	// Don't add peers from the config file when in regression test mode.
	if preCfg.RegressionTest && len(cfg.AddPeers) > 0 {
		cfg.AddPeers = nil
	}

	// Parse command line options again to ensure they take precedence.
	//remainingArgs, err := parser.Parse()
	//if err != nil {
	//	if e, ok := err.(*flags.Error); !ok || e.Type != flags.ErrHelp {
	//		logger.Errorf("%s", usageMessage)
	//	}
	//	return nil, nil, err
	//}

	// Create the home directory if it doesn't already exist.
	funcName := "loadConfig"

	// Multiple networks can't be selected simultaneously.
	numNets := 0
	// Count number of network flags passed; assign active network params
	// while we're at it
	if cfg.TestNet3 {
		numNets++
		activeNetParams = &testNet3Params
	}
	if cfg.RegressionTest {
		numNets++
		activeNetParams = &regressionNetParams
	}
	if cfg.SimNet {
		numNets++
		// Also disable dns seeding on the simulation test network.
		activeNetParams = &simNetParams
		cfg.DisableDNSSeed = true
	}
	if numNets > 1 {
		str := "%s: The testnet, regtest, segnet, and simnet params " +
			"can't be used together -- choose one of the four"
		err := fmt.Errorf(str, funcName)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Set the default policy for relaying non-standard transactions
	// according to the default of the active network. The set
	// configuration value takes precedence over the default value for the
	// selected network.
	relayNonStd := activeNetParams.RelayNonStdTxs
	switch {
	case cfg.RelayNonStd && cfg.RejectNonStd:
		str := "%s: rejectnonstd and relaynonstd cannot be used " +
			"together -- choose only one"
		err := fmt.Errorf(str, funcName)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	case cfg.RejectNonStd:
		relayNonStd = false
	case cfg.RelayNonStd:
		relayNonStd = true
	}
	cfg.RelayNonStd = relayNonStd

	// Validate profile port number
	if cfg.Profile != "" {
		profilePort, err := strconv.Atoi(cfg.Profile)
		if err != nil || profilePort < 1024 || profilePort > 65535 {
			str := "%s: The profile port must be between 1024 and 65535"
			err := fmt.Errorf(str, funcName)
			logger.Errorf("%v", err)
			logger.Errorf("%s", usageMessage)
			return nil, nil, err
		}
	}

	// Don't allow ban durations that are too short.
	if cfg.BanDuration < time.Second {
		str := "%s: The banduration option may not be less than 1s -- parsed [%v]"
		err := fmt.Errorf(str, funcName, cfg.BanDuration)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	if cfg.Prune && cfg.PruneDepth < minPruneDepth {
		err := fmt.Errorf("%s: The pruneheight option may not be less than %d -- parsed [%d]", funcName, minPruneDepth, cfg.PruneDepth)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Validate any given whitelisted IP addresses and networks.
	if len(cfg.Whitelists) > 0 {
		var ip net.IP
		cfg.whitelists = make([]*net.IPNet, 0, len(cfg.Whitelists))

		for _, addr := range cfg.Whitelists {
			_, ipnet, err := net.ParseCIDR(addr)
			if err != nil {
				ip = net.ParseIP(addr)
				if ip == nil {
					str := "%s: The whitelist value of '%s' is invalid"
					err = fmt.Errorf(str, funcName, addr)
					logger.Errorf("%v", err)
					logger.Errorf("%s", usageMessage)
					return nil, nil, err
				}
				var bits int
				if ip.To4() == nil {
					// IPv6
					bits = 128
				} else {
					bits = 32
				}
				ipnet = &net.IPNet{
					IP:   ip,
					Mask: net.CIDRMask(bits, bits),
				}
			}
			cfg.whitelists = append(cfg.whitelists, ipnet)
		}
	}

	// --addPeer and --connect do not mix.
	if len(cfg.AddPeers) > 0 && len(cfg.ConnectPeers) > 0 {
		str := "%s: the --addpeer and --connect options can not be " +
			"mixed"
		err := fmt.Errorf(str, funcName)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// --proxy or --connect without --listen disables listening.
	if (cfg.Proxy != "" || len(cfg.ConnectPeers) > 0) &&
		len(cfg.Listeners) == 0 {
		cfg.DisableListen = true
	}

	// Connect means no DNS seeding.
	if len(cfg.ConnectPeers) > 0 {
		cfg.DisableDNSSeed = true
	}

	// Add the default listener if none were specified. The default
	// listener is all addresses on the listen port for the network
	// we are to connect to.
	if len(cfg.Listeners) == 0 {
		cfg.Listeners = []string{
			net.JoinHostPort("", activeNetParams.DefaultPort),
		}
	}

	var err error
	// Validate the minrelaytxfee.
	cfg.minRelayTxFee, err = bsvutil.NewAmount(cfg.MinRelayTxFee)
	if err != nil {
		str := "%s: invalid minrelaytxfee: %v"
		err := fmt.Errorf(str, funcName, err)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Limit the max orphan count to a sane vlue.
	if cfg.MaxOrphanTxs < 0 {
		str := "%s: The maxorphantx option may not be less than 0 " +
			"-- parsed [%d]"
		err := fmt.Errorf(str, funcName, cfg.MaxOrphanTxs)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Excessive blocksize cannot be set less than the default, but it can be higher.
	cfg.ExcessiveBlockSize = maxUint64(cfg.ExcessiveBlockSize, defaultExcessiveBlockSize)

	// Limit the max block size to a sane value.
	blockMaxSizeMax := cfg.ExcessiveBlockSize - 1000
	if cfg.BlockMaxSize < blockMaxSizeMin || cfg.BlockMaxSize > blockMaxSizeMax {
		str := "%s: The blockmaxsize option must be in between %d and %d -- parsed [%d]"
		err = fmt.Errorf(str, funcName, blockMaxSizeMin,
			blockMaxSizeMax, cfg.BlockMaxSize)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}
	// Limit the block priority and minimum block sizes to max block size.
	cfg.BlockPrioritySize = minUint64(cfg.BlockPrioritySize, cfg.BlockMaxSize)
	cfg.BlockMinSize = minUint64(cfg.BlockMinSize, cfg.BlockMaxSize)

	// Prepend ExcessiveBlockSize signaling to the UserAgentComments
	cfg.UserAgentComments = append([]string{fmt.Sprintf("EB%.1f", float64(cfg.ExcessiveBlockSize)/1000000)}, cfg.UserAgentComments...)

	// Look for illegal characters in the user agent comments.
	for _, uaComment := range cfg.UserAgentComments {
		if strings.ContainsAny(uaComment, "/:()") {
			err := fmt.Errorf("%s: The following characters must not "+
				"appear in user agent comments: '/', ':', '(', ')'",
				funcName)
			logger.Errorf("%v", err)
			logger.Errorf("%s", usageMessage)
			return nil, nil, err
		}
	}

	// Check mining addresses are valid and saved parsed versions.
	cfg.miningAddrs = make([]bsvutil.Address, 0, len(cfg.MiningAddrs))
	for _, strAddr := range cfg.MiningAddrs {
		addr, err := bsvutil.DecodeAddress(strAddr, activeNetParams.Params)
		if err != nil {
			str := "%s: mining address '%s' failed to decode: %v"
			err := fmt.Errorf(str, funcName, strAddr, err)
			logger.Errorf("%v", err)
			logger.Errorf("%s", usageMessage)
			return nil, nil, err
		}
		if !addr.IsForNet(activeNetParams.Params) {
			str := "%s: mining address '%s' is on the wrong network"
			err := fmt.Errorf(str, funcName, strAddr)
			logger.Errorf("%v", err)
			logger.Errorf("%s", usageMessage)
			return nil, nil, err
		}
		cfg.miningAddrs = append(cfg.miningAddrs, addr)
	}

	// Ensure there is at least one mining address when the generate flag is
	// set.
	if cfg.Generate && len(cfg.MiningAddrs) == 0 {
		str := "%s: the generate flag is set, but there are no mining " +
			"addresses specified "
		err := fmt.Errorf(str, funcName)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Add default port to all listener addresses if needed and remove
	// duplicate addresses.
	cfg.Listeners = normalizeAddresses(cfg.Listeners,
		activeNetParams.DefaultPort)

	// Add default port to all added peer addresses if needed and remove
	// duplicate addresses.
	cfg.AddPeers = normalizeAddresses(cfg.AddPeers,
		activeNetParams.DefaultPort)
	cfg.ConnectPeers = normalizeAddresses(cfg.ConnectPeers,
		activeNetParams.DefaultPort)

	// Check the checkpoints for syntax errors.
	cfg.addCheckpoints, err = parseCheckpoints(cfg.AddCheckpoints)
	if err != nil {
		str := "%s: Error parsing checkpoints: %v"
		err := fmt.Errorf(str, funcName, err)
		logger.Errorf("%v", err)
		logger.Errorf("%s", usageMessage)
		return nil, nil, err
	}

	// Setup dial and DNS resolution (lookup) functions depending on the
	// specified options.  The default is to use the standard
	// net.DialTimeout function as well as the system DNS resolver.  When a
	// proxy is specified, the dial function is set to the proxy specific
	// dial function and the lookup is set to use tor (unless --noonion is
	// specified in which case the system DNS resolver is used).
	cfg.dial = net.DialTimeout
	cfg.lookup = net.LookupIP
	if cfg.Proxy != "" {
		_, _, err := net.SplitHostPort(cfg.Proxy)
		if err != nil {
			str := "%s: Proxy address '%s' is invalid: %v"
			err := fmt.Errorf(str, funcName, cfg.Proxy, err)
			logger.Errorf("%v", err)
			logger.Errorf("%s", usageMessage)
			return nil, nil, err
		}

		proxy := &socks.Proxy{
			Addr:     cfg.Proxy,
			Username: cfg.ProxyUser,
			Password: cfg.ProxyPass,
		}
		cfg.dial = proxy.DialTimeout
	}

	// Warn about missing config file only after all other configuration is
	// done.  This prevents the warning on help messages and invalid
	// options.  Note this should go directly before the return.
	//if configFileError != nil {
	//	bsvdLog.Warnf("%v", configFileError)
	//}

	return &cfg, nil, nil
}

// bsvdDial connects to the address on the named network using the appropriate
// dial function depending on the address and configuration options.  For
// example, .onion addresses will be dialed using the onion specific proxy if
// one was specified, but will otherwise use the normal dial function (which
// could itself use a proxy or not).
func bsvdDial(addr net.Addr) (net.Conn, error) {
	return cfg.dial(addr.Network(), addr.String(), defaultConnectTimeout)
}

// bsvdLookup resolves the IP of the given host using the correct DNS lookup
// function depending on the configuration options.  For example, addresses will
// be resolved using tor when the --proxy flag was specified unless --noonion
// was also specified in which case the normal system DNS resolver will be used.
//
// Any attempt to resolve a tor address (.onion) will return an error since they
// are not intended to be resolved outside of the tor proxy.
func bsvdLookup(host string) ([]net.IP, error) {
	if strings.HasSuffix(host, ".onion") {
		return nil, fmt.Errorf("attempt to resolve tor address %s", host)
	}

	return cfg.lookup(host)
}
