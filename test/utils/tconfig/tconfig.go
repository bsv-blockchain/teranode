package tconfig

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type TConfigLoader func(s *TConfig) error

// TConfig hold the flat configurations for the testing
//
// TODO :
//
//	For specific arguments in cli
//	  --help : print help
//	  String() : print the config to various formats
type TConfig struct {
	// viper hold Viper instance as a helper to load configuration to TConfig structure
	viper *viper.Viper

	// Suite hold the meta information to configure for a test suite
	Suite ConfigSuite `mapstructure:"suite" json:"suite" yaml:"suite"`

	// LocalSystem hold the meta information to configure for a test localsystem
	LocalSystem ConfigLocalSystem `mapstructure:"localsystem" json:"localsystem" yaml:"localsystem"`

	// Teranode hold the informations for teranodes to be tested
	Teranode ConfigTeranode `mapstructure:"teranode" json:"teranode" yaml:"teranode"`
}

// LoadAllConfig return a functional loadding all configurations
// This is used in case user do not specify any config loader
func LoadAllConfig() TConfigLoader {
	return func(s *TConfig) error {
		allLoader := []TConfigLoader{
			LoadConfigSuite(),
			LoadConfigLocalSystem(),
			LoadConfigTeranode(),
		}

		for _, load := range allLoader {
			if err := load(s); err != nil {
				panic(err)
			}
		}

		return nil
	}
}

// LoadTConfig load configured values into TConfig structure
//
// kv is the key-value store used to override config that can be set programmatically
// If used, It overrides all the config being set in environment variables or config files
// If not, give it nil value
func LoadTConfig(kv map[string]any, loaders ...TConfigLoader) TConfig {
	c := TConfig{}
	c.initViper()

	// Set override config
	if len(kv) > 0 {
		for key, value := range kv {
			c.Set(key, value)
		}
	}

	// If user don't specify any loader, then load all
	if loaders == nil || len(loaders) < 1 {
		loadAll := LoadAllConfig()
		if err := loadAll(&c); err != nil {
			panic(err)
		}

		return c
	}

	// Use all loader to load configuration to TConfig
	for _, load := range loaders {
		if err := load(&c); err != nil {
			panic(err)
		}
	}

	return c
}

// StringYAML return the config string in yaml format
func (c *TConfig) StringYAML() string {
	strYAML, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintf("Error marshalling to YAML: %v\n", err)
	}

	return string(strYAML)
}

func (c *TConfig) Set(k string, v any) {
	c.viper.Set(k, v)
}

// InitViper initialize viper instance held in TConfig if it is nil
// This initialization allows the TConfig to load configurations from
// environment variables, from config files of different formats .env, .yaml, .json
func (c *TConfig) initViper() {
	if c.viper == nil {
		localFlags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
		localFlags.Usage = func() {}     // Disable undesired printout usage of flag
		localFlags.SetOutput(io.Discard) // Disable undesired printout usage of flag

		tconfigFile := localFlags.String("tconfig-file", "", "Path to the test configuration file")
		skipSetup := localFlags.Bool("skip-setup", false, "Skip Setting up local system")
		skipTeardown := localFlags.Bool("skip-teardown", false, "Skip Tearing down local system")
		helpOnly := localFlags.Bool("help", false, "Print Help message")

		if err := localFlags.Parse(os.Args[1:]); err != nil {
			fmt.Printf("WARNING parsing flags for tconfig : %v\n", err)
		}

		c.Suite.HelpOnly = *helpOnly

		c.viper = viper.New()
		// c.viper.SetEnvPrefix("tconfig") // Optional prefix key ( environment varialbe )
		c.viper.AutomaticEnv()
		c.viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

		// If --tconfig-file is specified, it has to be good config file
		if *tconfigFile != "" {
			if c.isEnvFile(*tconfigFile) {
				// Viper transforms my.var to MY_VAR only for environment variables
				// but not in .env fie. We use dotenv to load the .env file to environment variables
				// To make this transformation works for .env file
				// Note that dotenv does not override the environment variale, so it will
				// not break the order of priority
				if err := loadEnvFile(*tconfigFile); err != nil {
					panic(err)
				} else {
					c.Suite.TConfigFile = *tconfigFile
				}
			} else {
				c.viper.SetConfigFile(*tconfigFile)
				err := c.viper.ReadInConfig()

				if err != nil {
					panic(err)
				} else {
					c.Suite.TConfigFile = *tconfigFile
				}
			}
		}

		if skipSetup != nil && *skipSetup {
			// Force the skip setup as the same level of key/value setting
			c.Set(KeyLocalSystemSkipSetup, *skipSetup)
		}

		if skipTeardown != nil && *skipTeardown {
			// Force the skip teardown as the same level of key/value setting
			c.Set(KeyLocalSystemSkipTeardown, *skipTeardown)
		}
	}
}

// isEnvFile checks if the file is .env
func (c *TConfig) isEnvFile(f string) bool {
	ext := filepath.Ext(f)
	if len(ext) > 1 {
		return ext[1:] == "env"
	}

	return false
}

// loadEnvFile reads a .env file and sets environment variables.
// Lines starting with # are comments. Empty lines are skipped.
// Existing environment variables are not overridden.
func loadEnvFile(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		// Remove surrounding quotes
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
		}
		// Don't override existing env vars (same behavior as godotenv)
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}
