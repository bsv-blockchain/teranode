package util

import (
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"

	"github.com/ordishs/gocore"
)

var aerospikeConnectionMutex sync.Mutex
var aerospikeConnections map[string]*uaerospike.Client

var readMaxRetries int
var readTimeout time.Duration
var readSocketTimeout time.Duration
var readSleepBetweenRetries time.Duration
var readSleepMultiplier float64
var readExitFastOnExhaustedConnectionPool bool

var writeMaxRetries int
var writeTimeout time.Duration
var writeSocketTimeout time.Duration

var writeSleepBetweenRetries time.Duration
var writeSleepMultiplier float64
var writeExitFastOnExhaustedConnectionPool bool

var batchTotalTimeout time.Duration
var batchSocketTimeout time.Duration
var batchAllowInlineSSD bool
var concurrentNodes int

var aerospikePrometheusMetrics = map[string]prometheus.Counter{}

func init() {
	aerospikeConnections = make(map[string]*uaerospike.Client)
}

func GetAerospikeClient(logger ulogger.Logger, url *url.URL) (*uaerospike.Client, error) {
	logger = logger.New("uaero")

	aerospikeConnectionMutex.Lock()
	defer aerospikeConnectionMutex.Unlock()

	var err error
	client, found := aerospikeConnections[url.Host]
	if !found {
		logger.Infof("[AEROSPIKE] Creating aerospike client for host: %s", url.Host)
		client, err = getAerospikeClient(logger, url)
		if err != nil {
			return nil, err
		}
		aerospikeConnections[url.Host] = client
	} else {
		logger.Infof("[AEROSPIKE] Reusing aerospike client: %v", url.Host)
	}

	return client, nil
}

func getAerospikeClient(logger ulogger.Logger, url *url.URL) (*uaerospike.Client, error) {
	if len(url.Path) < 1 {
		return nil, errors.NewConfigurationError("aerospike namespace not found")
	}

	policy := aerospike.NewClientPolicy()

	if gocore.Config().GetBool("aerospike_useDefaultBasePolicies", false) {
		logger.Warnf("Using default aerospike connection (base) policies")
	} else {
		readPolicyUrl, err, found := gocore.Config().GetURL("aerospike_readPolicy")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no aerospike_readPolicy setting found")
		}

		logger.Infof("[Aerospike] readPolicy url %s", readPolicyUrl)
		readMaxRetries = getQueryInt(readPolicyUrl, "MaxRetries", aerospike.NewPolicy().MaxRetries, logger)
		readTimeout = getQueryDuration(readPolicyUrl, "TotalTimeout", aerospike.NewPolicy().TotalTimeout, logger)
		readSocketTimeout = getQueryDuration(readPolicyUrl, "SocketTimeout", aerospike.NewPolicy().SocketTimeout, logger)
		readSleepBetweenRetries = getQueryDuration(readPolicyUrl, "SleepBetweenRetries", aerospike.NewPolicy().SleepBetweenRetries, logger)
		readSleepMultiplier = getQueryFloat64(readPolicyUrl, "SleepMultiplier", aerospike.NewPolicy().SleepMultiplier, logger)
		readExitFastOnExhaustedConnectionPool = getQueryBool(readPolicyUrl, "ExitFastOnExhaustedConnectionPool", aerospike.NewPolicy().ExitFastOnExhaustedConnectionPool, logger)

		writePolicyUrl, err, found := gocore.Config().GetURL("aerospike_writePolicy")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no aerospike_writePolicy setting found")
		}

		logger.Infof("[Aerospike] writePolicy url %s", writePolicyUrl)
		writeMaxRetries = getQueryInt(writePolicyUrl, "MaxRetries", aerospike.NewWritePolicy(0, 0).MaxRetries, logger)
		writeTimeout = getQueryDuration(writePolicyUrl, "TotalTimeout", aerospike.NewWritePolicy(0, 0).TotalTimeout, logger)
		writeSocketTimeout = getQueryDuration(writePolicyUrl, "SocketTimeout", aerospike.NewWritePolicy(0, 0).SocketTimeout, logger)
		writeSleepBetweenRetries = getQueryDuration(writePolicyUrl, "SleepBetweenRetries", aerospike.NewPolicy().SleepBetweenRetries, logger)
		// TODO - remove the next line when this variable is actually used
		_ = writeSleepBetweenRetries
		writeSleepMultiplier = getQueryFloat64(writePolicyUrl, "SleepMultiplier", aerospike.NewPolicy().SleepMultiplier, logger)
		// TODO - remove the next line when this variable is actually used
		_ = writeSleepMultiplier
		writeExitFastOnExhaustedConnectionPool = getQueryBool(writePolicyUrl, "ExitFastOnExhaustedConnectionPool", aerospike.NewPolicy().ExitFastOnExhaustedConnectionPool, logger)
		// TODO - remove the next line when this variable is actually used
		_ = writeExitFastOnExhaustedConnectionPool

		// batching stuff
		batchPolicyUrl, err, found := gocore.Config().GetURL("aerospike_batchPolicy")
		if err != nil {
			panic(err)
		}
		if !found {
			panic("no aerospike_writePolicy setting found")
		}

		logger.Infof("[Aerospike] batchPolicy url %s", batchPolicyUrl)
		batchTotalTimeout = getQueryDuration(batchPolicyUrl, "TotalTimeout", aerospike.NewBatchPolicy().TotalTimeout, logger)
		batchSocketTimeout = getQueryDuration(batchPolicyUrl, "SocketTimeout", aerospike.NewBatchPolicy().SocketTimeout, logger)
		batchAllowInlineSSD = getQueryBool(batchPolicyUrl, "AllowInlineSSD", aerospike.NewBatchPolicy().AllowInlineSSD, logger)
		concurrentNodes = getQueryInt(batchPolicyUrl, "ConcurrentNodes", aerospike.NewBatchPolicy().ConcurrentNodes, logger)

		// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
		// todo optimize read policies
		// todo optimize write policies
		logger.Infof("[Aerospike] base/connection policy url %s", url)
		policy.LimitConnectionsToQueueSize = getQueryBool(url, "LimitConnectionsToQueueSize", policy.LimitConnectionsToQueueSize, logger)
		policy.ConnectionQueueSize = getQueryInt(url, "ConnectionQueueSize", policy.ConnectionQueueSize, logger)
		policy.MinConnectionsPerNode = getQueryInt(url, "MinConnectionsPerNode", policy.MinConnectionsPerNode, logger)
		policy.MaxErrorRate = getQueryInt(url, "MaxErrorRate", policy.MaxErrorRate, logger)
		policy.FailIfNotConnected = getQueryBool(url, "FailIfNotConnected", policy.FailIfNotConnected, logger)
		policy.Timeout = getQueryDuration(url, "Timeout", policy.Timeout, logger)
		policy.IdleTimeout = getQueryDuration(url, "IdleTimeout", policy.IdleTimeout, logger)
		policy.LoginTimeout = getQueryDuration(url, "LoginTimeout", policy.LoginTimeout, logger)
		policy.ErrorRateWindow = getQueryInt(url, "ErrorRateWindow", policy.ErrorRateWindow, logger)
		policy.OpeningConnectionThreshold = getQueryInt(url, "OpeningConnectionThreshold", policy.OpeningConnectionThreshold, logger)
	}

	if url.User != nil {
		policy.AuthMode = aerospike.AuthModeInternal

		policy.User = url.User.Username()
		var ok bool
		policy.Password, ok = url.User.Password()
		if !ok {
			policy.User = ""
			policy.Password = ""
		}
	}

	var hosts []*aerospike.Host
	urlHosts := strings.Split(url.Host, ",")
	for _, host := range urlHosts {
		hostParts := strings.Split(host, ":")
		if len(hostParts) == 2 {
			port, err := strconv.ParseInt(hostParts[1], 10, 32)
			if err != nil {
				return nil, errors.NewConfigurationError("invalid port %v", hostParts[1])
			}

			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: int(port),
			})
		} else if len(hostParts) == 1 {
			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: 3000,
			})
		} else {
			return nil, errors.NewConfigurationError("invalid host %v", host)
		}
	}

	logger.Debugf("url %s policy %#v\n", url, policy)

	// policy = aerospike.NewClientPolicy()
	client, err := uaerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}

	if gocore.Config().GetBool("aerospike_warmUp", true) {
		warmUp := getQueryInt(url, "WarmUp", 0, logger)
		cnxNum, err := client.WarmUp(warmUp)
		logger.Infof("Warmed up %d aerospike connections", cnxNum)
		if err != nil {
			return nil, err
		}
	}

	initStats(logger, client)

	return client, nil
}

func initStats(logger ulogger.Logger, client *uaerospike.Client) {
	var nonAlphanumericRegex, _ = regexp.Compile(`[^a-zA-Z0-9]+`)

	aerospikeStatsRefresh, _ := gocore.Config().GetInt("aerospike_statsRefresh", 5)
	aerospikeStatsRefreshInterval := time.Duration(aerospikeStatsRefresh) * time.Second

	go func() {
		for {
			if !client.IsConnected() {
				time.Sleep(1 * time.Second)
				continue
			}

			stats, err := client.Stats()
			if err != nil {
				logger.Errorf("Error getting aerospike stats: %s", err.Error())
				continue
			}

			// stats are: map[string]interface {} of
			// "server" -> map[string]interface{}
			// "cluster-aggregated-stats" -> map[string]interface{}
			// open-connections -> int16
			for key, stat := range stats {
				key := nonAlphanumericRegex.ReplaceAllString(key, "_")
				switch s := stat.(type) {
				case map[string]interface{}:
					for subKey, subStat := range s {
						subKey := nonAlphanumericRegex.ReplaceAllString(subKey, "_")
						prometheusKey := fmt.Sprintf("%s_%s", key, subKey)
						// create prometheus metric, if not exists
						if _, ok := aerospikePrometheusMetrics[prometheusKey]; !ok {
							aerospikePrometheusMetrics[prometheusKey] = promauto.NewCounter(
								prometheus.CounterOpts{
									Namespace: "aerospike_client",
									Subsystem: key,
									Name:      subKey,
									Help:      fmt.Sprintf("Aerospike stat %s:%s", key, subKey),
								},
							)
						}

						switch subStat := subStat.(type) {
						case int16:
							aerospikePrometheusMetrics[prometheusKey].Add(float64(subStat))
						case int:
							aerospikePrometheusMetrics[prometheusKey].Add(float64(subStat))
						case int32:
							aerospikePrometheusMetrics[prometheusKey].Add(float64(subStat))
						case int64:
							aerospikePrometheusMetrics[prometheusKey].Add(float64(subStat))
						case float32:
							aerospikePrometheusMetrics[prometheusKey].Add(float64(subStat))
						case float64:
							aerospikePrometheusMetrics[prometheusKey].Add(subStat)
						default:
							logger.Errorf("Unknown type for aerospike stat %s: %T", subKey, subStat)
						}
					}
				default:
					if _, ok := aerospikePrometheusMetrics[key]; !ok {
						aerospikePrometheusMetrics[key] = promauto.NewCounter(
							prometheus.CounterOpts{
								Namespace: "aerospike_client",
								Name:      key,
								Help:      fmt.Sprintf("Aerospike stat %s", key),
							},
						)
					}

					switch i := s.(type) {
					case int16:
						aerospikePrometheusMetrics[key].Add(float64(i))
					case int:
						aerospikePrometheusMetrics[key].Add(float64(i))
					case float64:
						aerospikePrometheusMetrics[key].Add(i)
					default:
						logger.Errorf("Unknown type for aerospike stat %s: %T", key, i)
					}
				}
			}

			time.Sleep(aerospikeStatsRefreshInterval)
		}
	}()
}

func getQueryBool(url *url.URL, key string, defaultValue bool, logger ulogger.Logger) bool {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%t [default]", key, defaultValue)
		return defaultValue
	}
	valueBool, err := strconv.ParseBool(value)
	if err != nil {
		logger.Fatalf("[Aerospike] Invalid value %s=%v", key, value)
	}
	logger.Infof("[Aerospike] %s=%t", key, valueBool)
	return valueBool
}

func getQueryInt(url *url.URL, key string, defaultValue int, logger ulogger.Logger) int {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%d [default]", key, defaultValue)
		return defaultValue
	}
	valueInt, err := strconv.Atoi(value)
	if err != nil {
		logger.Fatalf("[Aerospike] Invalid value %s=%v", key, value)
	}
	logger.Infof("[Aerospike] %s=%d", key, valueInt)
	return valueInt
}

func getQueryDuration(url *url.URL, key string, defaultValue time.Duration, logger ulogger.Logger) time.Duration {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%s [default]", key, defaultValue.String())
		return defaultValue
	}
	valueDuration, err := time.ParseDuration(value)
	if err != nil {
		logger.Fatalf("[Aerospike] Invalid value %s=%v", key, value)
	}
	logger.Infof("[Aerospike] %s=%s", key, valueDuration.String())
	return valueDuration
}

func getQueryFloat64(url *url.URL, key string, defaultValue float64, logger ulogger.Logger) float64 {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%f [default]", key, defaultValue)
		return defaultValue
	}
	valueFloat64, err := strconv.ParseFloat(value, 64)
	if err != nil {
		logger.Fatalf("[Aerospike] Invalid value %s=%v", key, value)
	}
	logger.Infof("[Aerospike] %s=%f", key, valueFloat64)
	return valueFloat64
}

// AerospikeReadPolicyOptions represents functional options for modifying Aerospike read policies.
type AerospikeReadPolicyOptions func(*aerospike.BasePolicy)

// WithTotalTimeout sets the total timeout for the Aerospike read policy.
func WithTotalTimeout(timeout time.Duration) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.TotalTimeout = timeout
	}
}

// WithSocketTimeout sets the socket timeout for the Aerospike read policy.
func WithSocketTimeout(timeout time.Duration) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.SocketTimeout = timeout
	}
}

// WithMaxRetries sets the maximum number of retries for the Aerospike read policy.
func WithMaxRetries(retries int) AerospikeReadPolicyOptions {
	return func(policy *aerospike.BasePolicy) {
		policy.MaxRetries = retries
	}
}

// GetAerospikeReadPolicy creates a new Aerospike read policy with the provided options applied. Used to manage
// default connection parameters
// If no options are provided, the policy will use the default values
func GetAerospikeReadPolicy(options ...AerospikeReadPolicyOptions) *aerospike.BasePolicy {
	readPolicy := aerospike.NewPolicy()

	if gocore.Config().GetBool("aerospike_useDefaultPolicies", false) {
		return readPolicy
	}

	readPolicy.MaxRetries = readMaxRetries
	readPolicy.TotalTimeout = readTimeout
	readPolicy.SocketTimeout = readSocketTimeout
	readPolicy.SleepBetweenRetries = readSleepBetweenRetries
	readPolicy.SleepMultiplier = readSleepMultiplier
	readPolicy.ExitFastOnExhaustedConnectionPool = readExitFastOnExhaustedConnectionPool

	// Apply the provided options
	for _, opt := range options {
		opt(readPolicy)
	}

	return readPolicy
}

// AerospikeWritePolicyOptions represents functional options for modifying Aerospike write policies.
type AerospikeWritePolicyOptions func(*aerospike.WritePolicy)

// WithTotalTimeoutWrite sets the total timeout for the Aerospike write policy.
func WithTotalTimeoutWrite(timeout time.Duration) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.TotalTimeout = timeout
	}
}

// WithExpiration sets the expiration for the Aerospike write policy
func WithExpiration(timeout uint32) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.Expiration = timeout
	}
}

// WithSocketTimeoutWrite sets the socket timeout for the Aerospike write policy.
func WithSocketTimeoutWrite(timeout time.Duration) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.SocketTimeout = timeout
	}
}

// WithMaxRetriesWrite sets the maximum number of retries for the Aerospike write policy.
func WithMaxRetriesWrite(retries int) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.BasePolicy.MaxRetries = retries
	}
}

// GetAerospikeWritePolicy creates a new Aerospike write policy with the provided options applied. Used to manage
// default connection parameters
// If no options are provided, the policy will use the default values
func GetAerospikeWritePolicy(generation, expiration uint32, options ...AerospikeWritePolicyOptions) *aerospike.WritePolicy {
	writePolicy := aerospike.NewWritePolicy(generation, expiration)

	if gocore.Config().GetBool("aerospike_useDefaultPolicies", false) {
		return writePolicy
	}

	writePolicy.MaxRetries = writeMaxRetries
	writePolicy.TotalTimeout = writeTimeout
	writePolicy.SocketTimeout = writeSocketTimeout
	writePolicy.SleepBetweenRetries = readSleepBetweenRetries
	writePolicy.SleepMultiplier = readSleepMultiplier
	writePolicy.ExitFastOnExhaustedConnectionPool = readExitFastOnExhaustedConnectionPool
	writePolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	// Apply the provided options
	for _, opt := range options {
		opt(writePolicy)
	}

	return writePolicy
}

func GetAerospikeBatchPolicy() *aerospike.BatchPolicy {
	batchPolicy := aerospike.NewBatchPolicy()

	if gocore.Config().GetBool("aerospike_useDefaultPolicies", false) {
		return batchPolicy
	}

	batchPolicy.TotalTimeout = batchTotalTimeout
	batchPolicy.SocketTimeout = batchSocketTimeout
	batchPolicy.AllowInlineSSD = batchAllowInlineSSD
	batchPolicy.ConcurrentNodes = concurrentNodes

	return batchPolicy
}

func GetAerospikeBatchWritePolicy(generation, expiration uint32) *aerospike.BatchWritePolicy {
	batchWritePolicy := aerospike.NewBatchWritePolicy()

	if gocore.Config().GetBool("aerospike_useDefaultPolicies", false) {
		return batchWritePolicy
	}

	batchWritePolicy.Expiration = expiration
	batchWritePolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	return batchWritePolicy
}

func GetAerospikeBatchReadPolicy() *aerospike.BatchReadPolicy {
	batchReadPolicy := aerospike.NewBatchReadPolicy()

	if gocore.Config().GetBool("aerospike_useDefaultPolicies", false) {
		return batchReadPolicy
	}

	return batchReadPolicy
}
