package tconfig

// Key definition conformed to viper format
// It make the key path flexible for
//   - environment variable
//   - config file .env .yaml .json
//
//	key  variable.name is equivalent to VARIABLE_NAME
//
// These paths here are prefixed by s.viper.SetEnvPrefix("tconfig"). See func (s *TConfig) initViper()
//
// NOTE :
//   Use the key as defined here instead of using raw string
//   It befinit the strong type of the language and avoid bugs

const (
	// Keys for suite config
	KeySuiteTestID     = "suite.testid"
	KeySuiteName       = "suite.name"
	KeySuiteLogLevel   = "suite.loglevel"
	KeyInitBlockHeight = "suite.initblockheight"
	KeyIsLegacyTest    = "suite.islegacytest"

	// Keys for localsystem config
	KeyLocalSystemComposes      = "localsystem.composes"
	KeyLocalSystemTStoreURL     = "localsystem.tstoreurl"
	KeyLocalSystemTStoreRootDir = "localsystem.tstorerootdir"
	KeyLocalSystemDataDir       = "localsystem.datadir"
	KeyLocalSystemSkipSetup     = "localsystem.skipsetup"
	KeyLocalSystemSkipTeardown  = "localsystem.skipteardown"

	// Keys for Teranodes setup
	KeyTeranodeContexts     = "teranode.contexts"
	KeyURLBlobBlockstores   = "teranode.urlblobblockstores"
	KeyURLBlobSubtreestores = "teranode.urlblobsubtreestores"
)
