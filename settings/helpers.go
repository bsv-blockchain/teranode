package settings

import (
	"net/url"
	"strconv"
	"time"

	"github.com/ordishs/gocore"
)

func getString(key, defaultValue string) string {
	value, found := gocore.Config().Get(key)
	if !found {
		return defaultValue
	}

	return value
}

func getMultiString(key, sep string, defaultValues []string) []string {
	value, _ := gocore.Config().GetMulti(key, sep, defaultValues)

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

func getFloat64(key string, defaultValue float64) float64 {
	strVal, found := gocore.Config().Get(key, "")
	value, err := strconv.ParseFloat(strVal, 64)

	if !found || err != nil {
		return defaultValue
	}

	return value
}

func getDuration(key string, defaultValue ...time.Duration) time.Duration {
	str, ok := gocore.Config().Get(key)
	if str == "" || !ok {
		if len(defaultValue) > 0 {
			return defaultValue[0]
		}

		return 0
	}

	d, err := time.ParseDuration(str)
	if err != nil {
		return 0
	}

	return d
}
