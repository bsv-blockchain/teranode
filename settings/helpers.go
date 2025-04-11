package settings

import (
	"net/url"
	"time"

	"github.com/ordishs/gocore"
)

func getString(key, defaultValue string, alternativeContext ...string) string {
	value, _ := gocore.Config(alternativeContext...).Get(key, defaultValue)

	return value
}

func getMultiString(key, sep string, defaultValues []string, alternativeContext ...string) []string {
	value, _ := gocore.Config(alternativeContext...).GetMulti(key, sep, defaultValues)

	return value
}

func getInt(key string, defaultValue int, alternativeContext ...string) int {
	value, _ := gocore.Config(alternativeContext...).GetInt(key, defaultValue)

	return value
}

func getInt32(key string, defaultValue int32, alternativeContext ...string) int32 {
	value, _ := gocore.Config(alternativeContext...).GetInt32(key, defaultValue)

	return value
}

func getURL(key string, defaultValue string, alternativeContext ...string) *url.URL {
	value, _, _ := gocore.Config(alternativeContext...).GetURL(key, defaultValue)

	return value
}

func getBool(key string, defaultValue bool, alternativeContext ...string) bool {
	return gocore.Config(alternativeContext...).GetBool(key, defaultValue)
}

func getFloat64(key string, defaultValue float64, alternativeContext ...string) float64 {
	value, _ := gocore.Config(alternativeContext...).GetFloat64(key, defaultValue)

	return value
}

func getDuration(key string, defaultValue time.Duration, alternativeContext ...string) time.Duration {
	d, err, _ := gocore.Config(alternativeContext...).GetDuration(key, defaultValue)
	if err != nil {
		panic(err)
	}

	return d
}
