package tconfig

// ConfigLocalSystem hold the docker-compose files and all related information
// to deploy and manage the local system to be tested.
type ConfigLocalSystem struct {
	// Composes hold the list of docker-compose files to be up when initiate suites locally
	// If empty, then the test suites skip initialize the docker-compose
	Composes []string `mapstructure:"composes" json:"composes" yaml:"composes"`

	// DataDir configure the base path to store/share test data on local system.
	// This is considered as the 'master' directory, all test data path are to be customize inside it
	DataDir string `mapstructure:"datadir" json:"datadir" yaml:"datadir"`
}

// LoadConfigLocalSystem return the loader to load the ConfigLocalSystem
func LoadConfigLocalSystem() TConfigLoader {
	return func(s *TConfig) error {
		s.viper.SetDefault(KeyLocalSystemComposes, []string{"../../docker-compose.e2etest.yml"})
		s.LocalSystem.Composes = s.viper.GetStringSlice(KeyLocalSystemComposes)

		s.viper.SetDefault(KeyLocalSystemDataDir, "../../data/test")
		s.LocalSystem.DataDir = s.viper.GetString(KeyLocalSystemDataDir)

		return nil
	}
}
