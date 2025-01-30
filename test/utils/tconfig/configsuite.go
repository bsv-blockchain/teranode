package tconfig

import (
	"fmt"
	"time"
)

// ConfigSuite hold the meta information to configure for a test suite
type ConfigSuite struct {
	// TestID hold the uniquely defined id for a test suite
	// It is used to isolate namespace in local docker-compose setup
	TestID string `mapstructure:"testid" json:"testid" yaml:"testid"`

	// Name hold the test suite name
	// It is used for human readable id, not need to be unique
	Name string `mapstructure:"name" json:"name" yaml:"name"`

	// LogLevel define the log level for the test suite
	LogLevel string `mapstructure:"loglevel" json:"loglevel" yaml:"loglevel"`

	// TConfigFile hold the path to tconfig file
	TConfigFile string `mapstructure:"tconfigfile" json:"tconfigfile" yaml:"tconfigfile"`

	// InitBlockHeight tell the system to generate initial N blocks (uniquely works for regtest)
	InitBlockHeight uint32 `mapstructure:"initblockheight" json:"initblockheight" yaml:"initblockheight"`

	// IsLegacyTest tell if the test includes svnodes or not
	IsLegacyTest bool `mapstructure:"islegacytest" json:"islegacytest" yaml:"islegacytest"`

	// HelpOnly tell the program to print help only then exit
	HelpOnly bool `mapstructure:"helponly" json:"helponly" yaml:"helponly"`
}

// LoadConfigSuite return the loader to load the ConfigSuite
func LoadConfigSuite() TConfigLoader {
	return func(s *TConfig) error {
		s.viper.SetDefault(KeySuiteTestID, fmt.Sprintf("test_%s", time.Now().UTC().Format("20060102t150405z000000000")))
		s.Suite.TestID = s.viper.GetString(KeySuiteTestID)

		s.viper.SetDefault(KeySuiteName, "DefaultName")
		s.Suite.Name = s.viper.GetString(KeySuiteName)

		s.viper.SetDefault(KeySuiteLogLevel, "ERROR")
		s.Suite.LogLevel = s.viper.GetString(KeySuiteLogLevel)

		s.viper.SetDefault(KeyInitBlockHeight, uint32(101))
		s.Suite.InitBlockHeight = s.viper.GetUint32(KeyInitBlockHeight)

		s.viper.SetDefault(KeyIsLegacyTest, false)
		s.Suite.IsLegacyTest = s.viper.GetBool(KeyIsLegacyTest)

		return nil
	}
}
