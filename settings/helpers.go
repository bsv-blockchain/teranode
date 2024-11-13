package settings

import (
	"net/url"

	"github.com/ordishs/gocore"
)

func getString(key, defaultValue string) string {
	value, found := gocore.Config().Get(key)
	if !found {
		return defaultValue
	}

	return value
}

func getMultiString(key, defaultValue string) []string {
	value, _ := gocore.Config().GetMulti(key, defaultValue)

	return value
}

func getInt(key string, defaultValue int) int {
	value, found := gocore.Config().GetInt(key)
	if !found {
		return defaultValue
	}

	return value
}

func getURL(key, defaultValue string) *url.URL {
	value, _, _ := gocore.Config().GetURL(key, defaultValue)

	return value
}

func getBool(key string, defaultValue bool) bool {
	return gocore.Config().GetBool(key, defaultValue)
}
