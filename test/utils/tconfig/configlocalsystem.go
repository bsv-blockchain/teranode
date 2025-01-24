package tconfig

// ConfigLocalSystem hold the docker-compose files and all related information
// to deploy and manage the local system to be tested.
type ConfigLocalSystem struct {
	// Composes hold the list of docker-compose files to be up when initiate suites locally
	// If not empty, then the test engine will know to setup and teardown the local system
	// accordingly to the compose files
	Composes []string `mapstructure:"composes" json:"composes" yaml:"composes"`

	// TStoreURL is URL in format host:port for the TStore service
	TStoreURL string `mapstructure:"tstoreurl" json:"tstoreurl" yaml:"tstoreurl"`

	// TStoreRootDir is the root dir to setup for the TStore service
	// The directory has to exist at initialization of the TStore service
	TStoreRootDir string `mapstructure:"tstorerootdir" json:"tstorerootdir" yaml:"tstorerootdir"`

	// DataDir configure the base path to store/share test data on local system.
	// This is considered as the 'master' directory, all test data path are to be customize inside it
	// TODO :
	//    This config is to be removed once the TStore service is in place. All test data directory
	//    will be used with relative path. The root part is managed by TStore service.
	DataDir string `mapstructure:"datadir" json:"datadir" yaml:"datadir"`

	// SkipSetup is used to force the test engine to skip setting up the local system
	// even the list of compose files is not empty
	// It is useful when tester want to rerun the same tests without reinitializing the system
	SkipSetup bool `mapstructure:"skipsetup" json:"skipsetup" yaml:"skipsetup"`

	// SkipTeardown is used to force the test engine to skip tearing down the local system
	// even the list of compose files is not empty
	// It is useful when tester wish to not shutting down the system after the test finishes
	SkipTeardown bool `mapstructure:"skipteardown" json:"skipteardown" yaml:"skipteardown"`
}

// LoadConfigLocalSystem return the loader to load the ConfigLocalSystem
func LoadConfigLocalSystem() TConfigLoader {
	return func(s *TConfig) error {
		s.viper.SetDefault(KeyLocalSystemComposes, []string{"../../docker-compose.e2etest.yml"})
		s.LocalSystem.Composes = s.viper.GetStringSlice(KeyLocalSystemComposes)

		s.viper.SetDefault(KeyLocalSystemSkipSetup, false)
		s.LocalSystem.SkipSetup = s.viper.GetBool(KeyLocalSystemSkipSetup)

		s.viper.SetDefault(KeyLocalSystemSkipTeardown, false)
		s.LocalSystem.SkipTeardown = s.viper.GetBool(KeyLocalSystemSkipTeardown)

		s.viper.SetDefault(KeyLocalSystemTStoreURL, ":50051")
		s.LocalSystem.TStoreURL = s.viper.GetString(KeyLocalSystemTStoreURL)

		s.viper.SetDefault(KeyLocalSystemTStoreRootDir, "/data/test")
		s.LocalSystem.TStoreRootDir = s.viper.GetString(KeyLocalSystemTStoreRootDir)

		s.viper.SetDefault(KeyLocalSystemDataDir, "../../data/test")
		s.LocalSystem.DataDir = s.viper.GetString(KeyLocalSystemDataDir)

		return nil
	}
}
