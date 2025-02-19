package tconfig

import (
	"fmt"
)

// ConfigTeranode hold the information for all teranode services to be tested
// It usually based on the universal settings file for the teranode, with defining
// multiple settings context. Each setting context get the particular context setting
// for the corresponding teranode.
type ConfigTeranode struct {
	// Contexts hold the list of settings contexts extracted from the universal settings file for teranode
	// Each context correspond to a particular settings for a teranode
	//
	// These values are to be populated to the environment SETTINGS_CONTEXT_<i> to get the
	// ith setting context for the ith node
	Contexts []string `mapstructure:"contexts" json:"contexts" yaml:"contexts"`

	// URLBlobBlockstores hold the url to the block store blob http server (host:port)
	// for all teranodes in docker-compose. It allow data access remotely using blob http client
	URLBlobBlockstores []string `mapstructure:"urlblobblockstores" json:"urlblobblockstores" yaml:"urlblobblockstores"`

	// URLBlobSubtreestores hold the url to the subtree store blob http server (host:port)
	// for all teranodes in docker-compose. It allow data access remotely using blob http client
	URLBlobSubtreestores []string `mapstructure:"urlblobsubtreestores" json:"urlblobsubtreestores" yaml:"urlblobsubtreestores"`
}

// SettingsMap return the transformed context list to the map, to be used in environments for docker-compose.
// The transformation is from a list of values, give the map with the key SETTINGS_CONTEXT_<i> for the value.
//
// Example
//
//	["contextA", "contextB"]  --> map[string]string{ "SETTINGS_CONTEXT_0": "contextA", SETTINGS_CONTEXT_1": "contextB" }
func (ct *ConfigTeranode) SettingsMap() map[string]string {
	ret := make(map[string]string)

	for i, val := range ct.Contexts {
		key := fmt.Sprintf("SETTINGS_CONTEXT_%v", i+1)
		ret[key] = val
	}

	return ret
}

// LoadConfigTeranode return the loader to load the ConfigTeranode
func LoadConfigTeranode() TConfigLoader {
	return func(s *TConfig) error {
		s.viper.SetDefault(KeyTeranodeContexts, []string{"docker.teranode1.test", "docker.teranode2.test", "docker.teranode3.test"})
		s.Teranode.Contexts = s.viper.GetStringSlice(KeyTeranodeContexts)

		s.viper.SetDefault(KeyURLBlobBlockstores, []string{"127.0.0.1:8081", "127.0.0.1:8082", "127.0.0.1:8083"})
		s.Teranode.URLBlobBlockstores = s.viper.GetStringSlice(KeyURLBlobBlockstores)

		s.viper.SetDefault(KeyURLBlobSubtreestores, []string{"127.0.0.1:8084", "127.0.0.1:8085", "127.0.0.1:8086"})
		s.Teranode.URLBlobSubtreestores = s.viper.GetStringSlice(KeyURLBlobSubtreestores)

		return nil
	}
}
