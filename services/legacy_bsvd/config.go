// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package legacy

import (
	_ "embed"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/database"
	_ "github.com/bitcoin-sv/ubsv/services/legacy/database/ffldb"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"

	flags "github.com/jessevdk/go-flags"
)

//go:embed sample-bsvd.conf
var configData []byte

const (
	defaultConfigFilename          = "bsvd.conf"
	defaultDataDirname             = "data"
	defaultLogLevel                = "info"
	defaultLogDirname              = "logs"
	defaultLogFilename             = "bsvd.log"
	defaultMaxPeers                = 125
	defaultMaxPeersPerIP           = 5
	defaultBanDuration             = time.Hour * 24
	defaultBanThreshold            = 100
	defaultConnectTimeout          = time.Second * 30
	defaultDbType                  = "ffldb"
	defaultFreeTxRelayLimit        = 15.0
	defaultTrickleInterval         = peer.DefaultTrickleInterval
	defaultExcessiveBlockSize      = 128000000
	defaultBlockMinSize            = 0
	defaultBlockMaxSize            = 750000
	blockMaxSizeMin                = 1000
	defaultGenerate                = false
	defaultMaxOrphanTransactions   = 100
	defaultMaxOrphanTxSize         = 100000
	defaultSigCacheMaxSize         = 100000
	defaultTxIndex                 = false
	defaultAddrIndex               = false
	defaultUtxoCacheMaxSizeMiB     = 450
	defaultMinSyncPeerNetworkSpeed = 51200
	defaultPruneDepth              = 4320
	defaultTargetOutboundPeers     = uint32(8)
	minPruneDepth                  = 288
)

var (
	defaultHomeDir, _ = gocore.Config().Get("legacy_workingDir", os.TempDir())
	defaultConfigFile = filepath.Join(defaultHomeDir, defaultConfigFilename)
	defaultDataDir    = filepath.Join(defaultHomeDir, defaultDataDirname)
	knownDbTypes      = database.SupportedDrivers()
	defaultLogDir     = filepath.Join(defaultHomeDir, defaultLogDirname)
)

// runServiceCommand is only set to a real function on Windows.  It is used
// to parse and execute service commands specified via the -s flag.
var runServiceCommand func(string) error

// minUint64 is a helper function to return the minimum of two uint64s.
// This avoids a math import and the need to cast to floats.
func minUint64(a, b uint64) uint64 {
	if a < b {
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
	ConfigFile              string        `short:"C" long:"configfile" description:"Path to configuration file"`
	DataDir                 string        `short:"b" long:"datadir" description:"Directory to store data"`
	LogDir                  string        `long:"logdir" description:"Directory to log output."`
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
	OnionProxy              string        `long:"onion" description:"Connect to tor hidden services via SOCKS5 proxy (eg. 127.0.0.1:9050)"`
	OnionProxyUser          string        `long:"onionuser" description:"Username for onion proxy server"`
	OnionProxyPass          string        `long:"onionpass" default-mask:"-" description:"Password for onion proxy server"`
	NoOnion                 bool          `long:"noonion" description:"Disable connecting to tor hidden services"`
	TorIsolation            bool          `long:"torisolation" description:"Enable Tor stream isolation by randomizing user credentials for each connection."`
	TestNet3                bool          `long:"testnet" description:"Use the test network"`
	RegressionTest          bool          `long:"regtest" description:"Use the regression test network"`
	SimNet                  bool          `long:"simnet" description:"Use the simulation test network"`
	AddCheckpoints          []string      `long:"addcheckpoint" description:"Add a custom checkpoint.  Format: '<height>:<hash>'"`
	DisableCheckpoints      bool          `long:"nocheckpoints" description:"Disable built-in checkpoints.  Don't do this unless you know what you're doing."`
	DbType                  string        `long:"dbtype" description:"Database backend to use for the Block Chain"`
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
	DropCfIndex             bool          `long:"dropcfindex" description:"Deletes the index used for committed filtering (CF) support from the database on start up and then exits."`
	SigCacheMaxSize         uint          `long:"sigcachemaxsize" description:"The maximum number of entries in the signature verification cache"`
	UtxoCacheMaxSizeMiB     uint          `long:"utxocachemaxsize" description:"The maximum size in MiB of the UTXO cache"`
	BlocksOnly              bool          `long:"blocksonly" description:"Do not accept transactions from remote peers."`
	TxIndex                 bool          `long:"txindex" description:"Maintain a full hash-based transaction index which makes all transactions available via the getrawtransaction RPC"`
	DropTxIndex             bool          `long:"droptxindex" description:"Deletes the hash-based transaction index from the database on start up and then exits."`
	AddrIndex               bool          `long:"addrindex" description:"Maintain a full address-based transaction index which makes the searchrawtransactions RPC available"`
	DropAddrIndex           bool          `long:"dropaddrindex" description:"Deletes the address-based transaction index from the database on start up and then exits."`
	RelayNonStd             bool          `long:"relaynonstd" description:"Relay non-standard transactions regardless of the default settings for the active network."`
	RejectNonStd            bool          `long:"rejectnonstd" description:"Reject non-standard transactions regardless of the default settings for the active network."`
	Prune                   bool          `long:"prune" description:"Delete historical blocks from the chain. A buffer of blocks will be retained in case of a reorg."`
	PruneDepth              uint32        `long:"prunedepth" description:"The number of blocks to retain when running in pruned mode. Cannot be less than 288."`
	TargetOutboundPeers     uint32        `long:"targetoutboundpeers" description:"number of outbound connections to maintain"`
	ReIndexChainState       bool          `long:"reindexchainstate" description:"Rebuild the UTXO database from currently indexed blocks on disk."`
	lookup                  func(string) ([]net.IP, error)
	oniondial               func(string, string, time.Duration) (net.Conn, error)
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

// cleanAndExpandPath expands environment variables and leading ~ in the
// passed path, cleans the result, and returns it.
func cleanAndExpandPath(path string) string {
	// Expand initial ~ to OS specific home directory.
	if strings.HasPrefix(path, "~") {
		homeDir := filepath.Dir(defaultHomeDir)
		path = strings.Replace(path, "~", homeDir, 1)
	}

	// NOTE: The os.ExpandEnv doesn't work with Windows-style %VARIABLE%,
	// but they variables can still be expanded via POSIX-style $VARIABLE.
	return filepath.Clean(os.ExpandEnv(path))
}

// validLogLevel returns whether or not logLevel is a valid debug log level.
func validLogLevel(logLevel string) bool {
	switch logLevel {
	case "trace":
		fallthrough
	case "debug":
		fallthrough
	case "info":
		fallthrough
	case "warn":
		fallthrough
	case "error":
		fallthrough
	case "critical":
		return true
	}
	return false
}

// supportedSubsystems returns a sorted slice of the supported subsystems for
// logging purposes.
func supportedSubsystems() []string {
	// Convert the subsystemLoggers map keys to a slice.
	subsystems := make([]string, 0, len(subsystemLoggers))
	for subsysID := range subsystemLoggers {
		subsystems = append(subsystems, subsysID)
	}

	// Sort the subsystems for stable display.
	sort.Strings(subsystems)
	return subsystems
}

// validDbType returns whether or not dbType is a supported database type.
func validDbType(dbType string) bool {
	for _, knownType := range knownDbTypes {
		if dbType == knownType {
			return true
		}
	}

	return false
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

// filesExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// newConfigParser returns a new command line flags parser.
func newConfigParser(cfg *config, so *serviceOptions, options flags.Options) *flags.Parser {
	return flags.NewParser(cfg, options)
}

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
func loadConfig() (*config, error) {
	// Default config.
	cfg := config{
		ConfigFile:              defaultConfigFile,
		DebugLevel:              defaultLogLevel,
		MaxPeers:                defaultMaxPeers,
		MaxPeersPerIP:           defaultMaxPeersPerIP,
		MinSyncPeerNetworkSpeed: defaultMinSyncPeerNetworkSpeed,
		BanDuration:             defaultBanDuration,
		BanThreshold:            defaultBanThreshold,
		DataDir:                 defaultDataDir,
		LogDir:                  defaultLogDir,
		DbType:                  defaultDbType,
		ExcessiveBlockSize:      defaultExcessiveBlockSize,
		FreeTxRelayLimit:        defaultFreeTxRelayLimit,
		TrickleInterval:         defaultTrickleInterval,
		BlockMinSize:            defaultBlockMinSize,
		BlockMaxSize:            defaultBlockMaxSize,
		MaxOrphanTxs:            defaultMaxOrphanTransactions,
		SigCacheMaxSize:         defaultSigCacheMaxSize,
		UtxoCacheMaxSizeMiB:     defaultUtxoCacheMaxSizeMiB,
		Generate:                defaultGenerate,
		TxIndex:                 defaultTxIndex,
		AddrIndex:               defaultAddrIndex,
		PruneDepth:              defaultPruneDepth,
		TargetOutboundPeers:     defaultTargetOutboundPeers,
	}

	connectPeers, ok := gocore.Config().GetMulti("legacy_connect_peers", "|")
	if ok {
		cfg.ConnectPeers = connectPeers
	}

	// Create the home directory if it doesn't already exist.
	funcName := "loadConfig"
	err := os.MkdirAll(defaultHomeDir, 0700)
	if err != nil {
		// Show a nicer error message if it's because a symlink is
		// linked to a directory that does not exist (probably because
		// it's not mounted).
		if e, ok := err.(*os.PathError); ok && os.IsExist(err) {
			if link, lerr := os.Readlink(e.Path); lerr == nil {
				str := "is symlink %s -> %s mounted?"
				err = fmt.Errorf(str, e.Path, link)
			}
		}

		str := "%s: Failed to create home directory: %v"
		err := fmt.Errorf(str, funcName, err)
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	// Set the default policy for relaying non-standard transactions
	// according to the default of the active network. The set
	// configuration value takes precedence over the default value for the
	// selected network.
	cfg.RelayNonStd = activeNetParams.RelayNonStdTxs

	// Append the network type to the data directory so it is "namespaced"
	// per network.  In addition to the block database, there are other
	// pieces of data that are saved to disk such as address manager state.
	// All data is specific to a network, so namespacing the data directory
	// means each individual piece of serialized data does not have to
	// worry about changing names per network and such.
	cfg.DataDir = cleanAndExpandPath(cfg.DataDir)
	cfg.DataDir = filepath.Join(cfg.DataDir, netName(activeNetParams))

	// Append the network type to the log directory so it is "namespaced"
	// per network in the same fashion as the data directory.
	cfg.LogDir = cleanAndExpandPath(cfg.LogDir)
	cfg.LogDir = filepath.Join(cfg.LogDir, netName(activeNetParams))

	// Add the default listener if none were specified. The default
	// listener is all addresses on the listen port for the network
	// we are to connect to.
	if len(cfg.Listeners) == 0 {
		cfg.Listeners = []string{
			net.JoinHostPort("", activeNetParams.DefaultPort),
		}
	}
	// Limit the block priority and minimum block sizes to max block size.
	cfg.BlockPrioritySize = minUint64(cfg.BlockPrioritySize, cfg.BlockMaxSize)
	cfg.BlockMinSize = minUint64(cfg.BlockMinSize, cfg.BlockMaxSize)

	// Prepend ExcessiveBlockSize signaling to the UserAgentComments
	cfg.UserAgentComments = append([]string{fmt.Sprintf("EB%.1f", float64(cfg.ExcessiveBlockSize)/1000000)}, cfg.UserAgentComments...)

	// Check the checkpoints for syntax errors.
	cfg.addCheckpoints, err = parseCheckpoints(cfg.AddCheckpoints)
	if err != nil {
		str := "%s: Error parsing checkpoints: %v"
		err := fmt.Errorf(str, funcName, err)
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	}

	// Setup dial and DNS resolution (lookup) functions depending on the
	// specified options.  The default is to use the standard
	// net.DialTimeout function as well as the system DNS resolver.  When a
	// proxy is specified, the dial function is set to the proxy specific
	// dial function and the lookup is set to use tor (unless --noonion is
	// specified in which case the system DNS resolver is used).
	cfg.dial = net.DialTimeout
	cfg.lookup = net.LookupIP

	return &cfg, err
}

// bsvdDial connects to the address on the named network using the appropriate
// dial function depending on the address and configuration options.  For
// example, .onion addresses will be dialed using the onion specific proxy if
// one was specified, but will otherwise use the normal dial function (which
// could itself use a proxy or not).
func bsvdDial(addr net.Addr) (net.Conn, error) {
	if strings.Contains(addr.String(), ".onion:") {
		return cfg.oniondial(addr.Network(), addr.String(),
			defaultConnectTimeout)
	}
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
