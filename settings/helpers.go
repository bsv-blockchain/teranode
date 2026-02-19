package settings

import (
	"net/url"
	"strconv"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/urlutil"
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

func getMultiStringMap(key, sep string, defaultValues []string, alternativeContext ...string) map[string]struct{} {
	slice := getMultiString(key, sep, defaultValues, alternativeContext...)

	m := make(map[string]struct{}, len(slice))
	for _, value := range slice {
		m[value] = struct{}{}
	}

	return m
}

func getInt(key string, defaultValue int, alternativeContext ...string) int {
	value, _ := gocore.Config(alternativeContext...).GetInt(key, defaultValue)

	return value
}

func getInt32(key string, defaultValue int32, alternativeContext ...string) int32 {
	value, _ := gocore.Config(alternativeContext...).GetInt32(key, defaultValue)

	return value
}

func getUint32(key string, defaultValue uint32, alternativeContext ...string) uint32 {
	value, _ := gocore.Config(alternativeContext...).GetUint32(key, defaultValue)

	return value
}

func getUint64(key string, defaultValue uint64, alternativeContext ...string) uint64 {
	value, _ := gocore.Config(alternativeContext...).GetUint64(key, defaultValue)

	return value
}

func getURL(key string, defaultValue string, alternativeContext ...string) *url.URL {
	str, _ := gocore.Config(alternativeContext...).Get(key, defaultValue)
	if str == "" {
		return nil
	}

	u, err := urlutil.ParseMultiHostURL(str)
	if err != nil {
		return nil
	}

	return u
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

func getPort(key string, defaultValue int, alternativeContext ...string) int {
	portPrefix := getString("PORT_PREFIX", "", alternativeContext...)
	port := getString(key, strconv.Itoa(defaultValue), alternativeContext...)

	portInt, err := strconv.Atoi(portPrefix + port)
	if err != nil {
		panic(err)
	}

	return portInt
}

func getIntSlice(key string, defaultValue []int, alternativeContext ...string) []int {
	// Get the string values
	strValues := getMultiString(key, ",", []string{}, alternativeContext...)

	// If no values found, return default
	if len(strValues) == 0 {
		return defaultValue
	}

	// Convert strings to ints
	result := make([]int, 0, len(strValues))

	for _, str := range strValues {
		val, err := strconv.Atoi(str)
		if err == nil {
			result = append(result, val)
		}
	}

	// If conversion failed for all values, return default
	if len(result) == 0 {
		return defaultValue
	}

	return result
}

// getPostgresPoolSettings reads service-specific PostgreSQL pool settings.
// Returns nil if no service-specific settings are configured (will use global defaults).
// Only returns a PostgresSettings pointer if at least one setting is explicitly configured.
func getPostgresPoolSettings(servicePrefix string, alternativeContext ...string) *PostgresSettings {
	maxOpenConns := getInt(servicePrefix+"_postgres_maxOpenConns", 0, alternativeContext...)
	maxIdleConns := getInt(servicePrefix+"_postgres_maxIdleConns", 0, alternativeContext...)
	connMaxLifetime := getDuration(servicePrefix+"_postgres_connMaxLifetime", 0, alternativeContext...)
	connMaxIdleTime := getDuration(servicePrefix+"_postgres_connMaxIdleTime", 0, alternativeContext...)
	retryMaxAttempts := getInt(servicePrefix+"_postgres_retryMaxAttempts", 0, alternativeContext...)
	retryBaseDelay := getDuration(servicePrefix+"_postgres_retryBaseDelay", 0, alternativeContext...)
	retryEnabled := getBool(servicePrefix+"_postgres_retryEnabled", false, alternativeContext...)

	// Only return settings if at least one is configured (non-zero)
	if maxOpenConns == 0 && maxIdleConns == 0 && connMaxLifetime == 0 && connMaxIdleTime == 0 &&
		retryMaxAttempts == 0 && retryBaseDelay == 0 {
		return nil
	}

	return &PostgresSettings{
		MaxOpenConns:     maxOpenConns,
		MaxIdleConns:     maxIdleConns,
		ConnMaxLifetime:  connMaxLifetime,
		ConnMaxIdleTime:  connMaxIdleTime,
		RetryMaxAttempts: retryMaxAttempts,
		RetryBaseDelay:   retryBaseDelay,
		RetryEnabled:     retryEnabled,
	}
}
