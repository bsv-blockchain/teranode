package settings

import (
	"net/url"
	"strconv"
	"time"

	"github.com/ordishs/gocore"
)

func getString(key, defaultValue string, alternativeContext ...string) string {
	value, found := gocore.Config(alternativeContext...).Get(key)
	if !found {
		return defaultValue
	}

	return value
}

func getMultiString(key, sep string, defaultValues []string, alternativeContext ...string) []string {
	value, _ := gocore.Config(alternativeContext...).GetMulti(key, sep, defaultValues)

	return value
}

func getInt(key string, defaultValue int, alternativeContext ...string) int {
	value, found := gocore.Config(alternativeContext...).GetInt(key)
	if !found {
		return defaultValue
	}

	return value
}

func getURL(key, defaultValue string, alternativeContext ...string) *url.URL {
	value, _, _ := gocore.Config(alternativeContext...).GetURL(key, defaultValue)

	return value
}

func getBool(key string, defaultValue bool, alternativeContext ...string) bool {
	return gocore.Config(alternativeContext...).GetBool(key, defaultValue)
}

func getFloat64(key string, defaultValue float64, alternativeContext ...string) float64 {
	strVal, found := gocore.Config(alternativeContext...).Get(key, "")
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
