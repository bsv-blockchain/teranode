package util

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/safemap"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
var batchMaxRetries int
var batchSleepBetweenRetries time.Duration
var batchSleepMultiplier float64
var concurrentNodes int

var queryMaxRetries int
var queryTotalTimeout time.Duration
var querySocketTimeout time.Duration
var querySleepBetweenRetries time.Duration
var querySleepMultiplier float64

var aerospikePrometheusMetrics = *safemap.New[string, prometheus.Counter]()
var aerospikePrometheusHistograms = *safemap.New[string, prometheus.Histogram]()

func init() {
	aerospikeConnections = make(map[string]*uaerospike.Client)
}

// GetAerospikeClient creates or retrieves a cached Aerospike client for the given URL.
// It configures connection policies, authentication, and connection pooling based on settings.
// Returns a thread-safe client instance that can be shared across goroutines.
func GetAerospikeClient(logger ulogger.Logger, url *url.URL, tSettings *settings.Settings) (*uaerospike.Client, error) {
	logger = logger.New("uaero")

	aerospikeConnectionMutex.Lock()
	defer aerospikeConnectionMutex.Unlock()

	var err error

	client, found := aerospikeConnections[url.Host]
	if !found {
		logger.Infof("[AEROSPIKE] Creating aerospike client for host: %s", url.Host)

		client, err = getAerospikeClient(logger, url, tSettings)
		if err != nil {
			return nil, err
		}

		aerospikeConnections[url.Host] = client
	} else {
		logger.Infof("[AEROSPIKE] Reusing aerospike client: %v", url.Host)
	}

	// increase buffer size to 256MB for large records
	aerospike.MaxBufferSize = 1024 * 1024 * 512 // 512MB

	return client, nil
}

func getAerospikeClient(logger ulogger.Logger, url *url.URL, tSettings *settings.Settings) (*uaerospike.Client, error) {
	if len(url.Path) < 1 {
		return nil, errors.NewConfigurationError("aerospike namespace not found")
	}

	policy := aerospike.NewClientPolicy()

	if tSettings.Aerospike.UseDefaultBasePolicies {
		logger.Warnf("Using default aerospike connection (base) policies")
	} else {
		readPolicyURL := tSettings.Aerospike.ReadPolicyURL
		if readPolicyURL == nil {
			return nil, errors.NewConfigurationError("no aerospike_readPolicy found")
		}

		logger.Infof("[Aerospike] readPolicy url %s", readPolicyURL)

		var err error

		readMaxRetries, err = getQueryInt(readPolicyURL, "MaxRetries", aerospike.NewPolicy().MaxRetries, logger)
		if err != nil {
			return nil, err
		}

		readTimeout, err = getQueryDuration(readPolicyURL, "TotalTimeout", aerospike.NewPolicy().TotalTimeout, logger)
		if err != nil {
			return nil, err
		}

		readSocketTimeout, err = getQueryDuration(readPolicyURL, "SocketTimeout", aerospike.NewPolicy().SocketTimeout, logger)
		if err != nil {
			return nil, err
		}

		readSleepBetweenRetries, err = getQueryDuration(readPolicyURL, "SleepBetweenRetries", aerospike.NewPolicy().SleepBetweenRetries, logger)
		if err != nil {
			return nil, err
		}

		readSleepMultiplier, err = getQueryFloat64(readPolicyURL, "SleepMultiplier", aerospike.NewPolicy().SleepMultiplier, logger)
		if err != nil {
			return nil, err
		}

		readExitFastOnExhaustedConnectionPool, err = getQueryBool(readPolicyURL, "ExitFastOnExhaustedConnectionPool", aerospike.NewPolicy().ExitFastOnExhaustedConnectionPool, logger)
		if err != nil {
			return nil, err
		}

		writePolicyURL := tSettings.Aerospike.WritePolicyURL
		if writePolicyURL == nil {
			return nil, errors.NewConfigurationError("no aerospike_writePolicy setting found")
		}

		logger.Infof("[Aerospike] writePolicy url %s", writePolicyURL)

		writeMaxRetries, err = getQueryInt(writePolicyURL, "MaxRetries", aerospike.NewWritePolicy(0, 0).MaxRetries, logger)
		if err != nil {
			return nil, err
		}

		writeTimeout, err = getQueryDuration(writePolicyURL, "TotalTimeout", aerospike.NewWritePolicy(0, 0).TotalTimeout, logger)
		if err != nil {
			return nil, err
		}

		writeSocketTimeout, err = getQueryDuration(writePolicyURL, "SocketTimeout", aerospike.NewWritePolicy(0, 0).SocketTimeout, logger)
		if err != nil {
			return nil, err
		}

		writeSleepBetweenRetries, err = getQueryDuration(writePolicyURL, "SleepBetweenRetries", aerospike.NewWritePolicy(0, 0).SleepBetweenRetries, logger)
		if err != nil {
			return nil, err
		}

		writeSleepMultiplier, err = getQueryFloat64(writePolicyURL, "SleepMultiplier", aerospike.NewWritePolicy(0, 0).SleepMultiplier, logger)
		if err != nil {
			return nil, err
		}

		writeExitFastOnExhaustedConnectionPool, err = getQueryBool(writePolicyURL, "ExitFastOnExhaustedConnectionPool", aerospike.NewPolicy().ExitFastOnExhaustedConnectionPool, logger)
		if err != nil {
			return nil, err
		}

		// batching stuff
		batchPolicyURL := tSettings.Aerospike.BatchPolicyURL
		if batchPolicyURL == nil {
			return nil, errors.NewConfigurationError("no aerospike_batchPolicy setting found")
		}

		logger.Infof("[Aerospike] batchPolicy url %s", batchPolicyURL)

		batchTotalTimeout, err = getQueryDuration(batchPolicyURL, "TotalTimeout", aerospike.NewBatchPolicy().TotalTimeout, logger)
		if err != nil {
			return nil, err
		}

		batchMaxRetries, err = getQueryInt(batchPolicyURL, "MaxRetries", aerospike.NewBatchPolicy().MaxRetries, logger)
		if err != nil {
			return nil, err
		}

		batchSleepMultiplier, err = getQueryFloat64(batchPolicyURL, "SleepMultiplier", aerospike.NewBatchPolicy().SleepMultiplier, logger)
		if err != nil {
			return nil, err
		}

		batchSleepBetweenRetries, err = getQueryDuration(batchPolicyURL, "SleepBetweenRetries", aerospike.NewBatchPolicy().SleepBetweenRetries, logger)
		if err != nil {
			return nil, err
		}

		batchSocketTimeout, err = getQueryDuration(batchPolicyURL, "SocketTimeout", aerospike.NewBatchPolicy().SocketTimeout, logger)
		if err != nil {
			return nil, err
		}

		batchAllowInlineSSD, err = getQueryBool(batchPolicyURL, "AllowInlineSSD", aerospike.NewBatchPolicy().AllowInlineSSD, logger)
		if err != nil {
			return nil, err
		}

		concurrentNodes, err = getQueryInt(batchPolicyURL, "ConcurrentNodes", aerospike.NewBatchPolicy().ConcurrentNodes, logger)
		if err != nil {
			return nil, err
		}

		// Query policy stuff
		queryPolicyURL := tSettings.Aerospike.QueryPolicyURL
		if queryPolicyURL == nil {
			// If no query policy is set, use default values
			queryMaxRetries = aerospike.NewQueryPolicy().MaxRetries
			queryTotalTimeout = aerospike.NewQueryPolicy().TotalTimeout
			querySocketTimeout = aerospike.NewQueryPolicy().SocketTimeout
			querySleepBetweenRetries = aerospike.NewQueryPolicy().SleepBetweenRetries
			querySleepMultiplier = aerospike.NewQueryPolicy().SleepMultiplier
		} else {
			logger.Infof("[Aerospike] queryPolicy url %s", queryPolicyURL)

			queryMaxRetries, err = getQueryInt(queryPolicyURL, "MaxRetries", aerospike.NewQueryPolicy().MaxRetries, logger)
			if err != nil {
				return nil, err
			}

			queryTotalTimeout, err = getQueryDuration(queryPolicyURL, "TotalTimeout", aerospike.NewQueryPolicy().TotalTimeout, logger)
			if err != nil {
				return nil, err
			}

			querySocketTimeout, err = getQueryDuration(queryPolicyURL, "SocketTimeout", aerospike.NewQueryPolicy().SocketTimeout, logger)
			if err != nil {
				return nil, err
			}

			querySleepBetweenRetries, err = getQueryDuration(queryPolicyURL, "SleepBetweenRetries", aerospike.NewQueryPolicy().SleepBetweenRetries, logger)
			if err != nil {
				return nil, err
			}

			querySleepMultiplier, err = getQueryFloat64(queryPolicyURL, "SleepMultiplier", aerospike.NewQueryPolicy().SleepMultiplier, logger)
			if err != nil {
				return nil, err
			}
		}

		// todo optimize these https://github.com/aerospike/aerospike-client-go/issues/256#issuecomment-479964112
		// todo optimize read policies
		// todo optimize write policies
		logger.Infof("[Aerospike] base/connection policy url %s", url)

		policy.LimitConnectionsToQueueSize, err = getQueryBool(url, "LimitConnectionsToQueueSize", policy.LimitConnectionsToQueueSize, logger)
		if err != nil {
			return nil, err
		}

		policy.ConnectionQueueSize, err = getQueryInt(url, "ConnectionQueueSize", policy.ConnectionQueueSize, logger)
		if err != nil {
			return nil, err
		}

		policy.MinConnectionsPerNode, err = getQueryInt(url, "MinConnectionsPerNode", policy.MinConnectionsPerNode, logger)
		if err != nil {
			return nil, err
		}

		policy.MaxErrorRate, err = getQueryInt(url, "MaxErrorRate", policy.MaxErrorRate, logger)
		if err != nil {
			return nil, err
		}

		policy.FailIfNotConnected, err = getQueryBool(url, "FailIfNotConnected", policy.FailIfNotConnected, logger)
		if err != nil {
			return nil, err
		}

		policy.Timeout, err = getQueryDuration(url, "Timeout", policy.Timeout, logger)
		if err != nil {
			return nil, err
		}

		policy.IdleTimeout, err = getQueryDuration(url, "IdleTimeout", policy.IdleTimeout, logger)
		if err != nil {
			return nil, err
		}

		policy.LoginTimeout, err = getQueryDuration(url, "LoginTimeout", policy.LoginTimeout, logger)
		if err != nil {
			return nil, err
		}

		policy.ErrorRateWindow, err = getQueryInt(url, "ErrorRateWindow", policy.ErrorRateWindow, logger)
		if err != nil {
			return nil, err
		}

		policy.OpeningConnectionThreshold, err = getQueryInt(url, "OpeningConnectionThreshold", policy.OpeningConnectionThreshold, logger)
		if err != nil {
			return nil, err
		}
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
		switch len(hostParts) {
		case 2:
			port, err := strconv.ParseInt(hostParts[1], 10, 32)
			if err != nil {
				return nil, errors.NewConfigurationError("invalid port %v", hostParts[1])
			}

			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: int(port),
			})
		case 1:
			hosts = append(hosts, &aerospike.Host{
				Name: hostParts[0],
				Port: 3000,
			})
		default:
			return nil, errors.NewConfigurationError("invalid host %v", host)
		}
	}

	logger.Debugf("url %s policy %#v\n", url, policy)

	// policy = aerospike.NewClientPolicy()
	client, err := uaerospike.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}

	if tSettings.Aerospike.WarmUp {
		warmUp, err := getQueryInt(url, "WarmUp", 0, logger)
		if err != nil {
			return nil, err
		}

		cnxNum, err := client.WarmUp(warmUp)
		logger.Infof("Warmed up %d aerospike connections", cnxNum)

		if err != nil {
			return nil, err
		}
	}

	initStats(logger, client, tSettings)

	return client, nil
}

func initStats(logger ulogger.Logger, client *uaerospike.Client, tSettings *settings.Settings) {
	var nonAlphanumericRegex = regexp.MustCompile(`[^a-zA-Z0-9]+`)

	aerospikeStatsRefreshInterval := tSettings.Aerospike.StatsRefreshDuration

	client.EnableMetrics(nil)

	aerospikeLatencyBuckets := func() []float64 {
		buckets := make([]float64, 24)
		base := 2.0 // microseconds

		for i := uint(1); i <= 24; i++ {
			shift := 1<<i - 1
			buckets[i-1] = base * float64(shift)
		}

		return buckets
	}()

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
						if _, ok := aerospikePrometheusMetrics.Get(prometheusKey); !ok {
							aerospikePrometheusMetrics.Set(prometheusKey, promauto.NewCounter(
								prometheus.CounterOpts{
									Namespace: "teranode",
									Subsystem: "aerospike_client_" + key,
									Name:      subKey,
									Help:      fmt.Sprintf("Aerospike stat %s:%s", key, subKey),
								},
							))
						}

						switch subStat := subStat.(type) {
						case int16:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(float64(subStat))
							}
						case int:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(float64(subStat))
							}
						case int32:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(float64(subStat))
							}
						case int64:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(float64(subStat))
							}
						case float32:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(float64(subStat))
							}
						case float64:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								counter.Add(subStat)
							}
						case map[string]interface{}:
							if counter, ok := aerospikePrometheusMetrics.Get(prometheusKey); ok {
								if f, ok := subStat["count"].(float64); ok {
									counter.Add(f)
								}
							}

							if buckets, ok := subStat["buckets"].([]interface{}); ok && len(buckets) == len(aerospikeLatencyBuckets) {
								histogramKey := "aerospike_client_histogram_" + key + "_" + subKey
								// create prometheus histogram, if not exists
								if _, ok := aerospikePrometheusHistograms.Get(histogramKey); !ok {
									aerospikePrometheusHistograms.Set(histogramKey, promauto.NewHistogram(
										prometheus.HistogramOpts{
											Namespace: "teranode",
											Subsystem: "aerospike_client_histogram_" + key,
											Name:      subKey,
											Help:      fmt.Sprintf("Aerospike histogram %s:%s", key, subKey),
											Buckets:   aerospikeLatencyBuckets,
										},
									))
								}

								histogram, ok := aerospikePrometheusHistograms.Get(histogramKey)
								if ok {
									for i, v := range buckets {
										count, ok := v.(float64)
										if !ok || count == 0 {
											continue
										}

										// For Prometheus, observe the upper bound value 'count' times
										// For the last bucket, use the last boundary (2^24)
										var value float64

										if i < len(aerospikeLatencyBuckets)-1 {
											value = aerospikeLatencyBuckets[i]
										} else {
											value = aerospikeLatencyBuckets[len(aerospikeLatencyBuckets)-1]
										}

										for j := 0; j < int(count); j++ {
											histogram.Observe(value)
										}
									}
								} else {
									logger.Warnf("Histogram %s not found", histogramKey)
								}
							}
						default:
							logger.Debugf("Unknown type for aerospike stat %s: %T", subKey, subStat)
						}
					}
				default:
					if _, ok := aerospikePrometheusMetrics.Get(key); !ok {
						aerospikePrometheusMetrics.Set(key, promauto.NewCounter(
							prometheus.CounterOpts{
								Namespace: "teranode",
								Subsystem: "aerospike_client",
								Name:      key,
								Help:      fmt.Sprintf("Aerospike stat %s", key),
							},
						))
					}

					switch i := s.(type) {
					case int16:
						if counter, ok := aerospikePrometheusMetrics.Get(key); ok {
							counter.Add(float64(i))
						}
					case int:
						if counter, ok := aerospikePrometheusMetrics.Get(key); ok {
							counter.Add(float64(i))
						}
					case float64:
						if counter, ok := aerospikePrometheusMetrics.Get(key); ok {
							counter.Add(i)
						}
					default:
						logger.Debugf("Unknown type for aerospike stat %s: %T", key, i)
					}
				}
			}

			time.Sleep(aerospikeStatsRefreshInterval)
		}
	}()
}

func getQueryBool(url *url.URL, key string, defaultValue bool, logger ulogger.Logger) (bool, error) {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%t [default]", key, defaultValue)
		return defaultValue, nil
	}

	valueBool, err := strconv.ParseBool(value)
	if err != nil {
		return defaultValue, errors.NewInvalidArgumentError("[Aerospike] Invalid value %s=%v", key, value, err)
	}

	logger.Infof("[Aerospike] %s=%t", key, valueBool)

	return valueBool, nil
}

func getQueryInt(url *url.URL, key string, defaultValue int, logger ulogger.Logger) (int, error) {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%d [default]", key, defaultValue)
		return defaultValue, nil
	}

	valueInt, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue, errors.NewInvalidArgumentError("[Aerospike] Invalid value %s=%v", key, value, err)
	}

	logger.Infof("[Aerospike] %s=%d", key, valueInt)

	return valueInt, nil
}

func getQueryDuration(url *url.URL, key string, defaultValue time.Duration, logger ulogger.Logger) (time.Duration, error) {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%s [default]", key, defaultValue.String())
		return defaultValue, nil
	}

	valueDuration, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue, errors.NewInvalidArgumentError("[Aerospike] Invalid value %s=%v", key, value, err)
	}

	logger.Infof("[Aerospike] %s=%s", key, valueDuration.String())

	return valueDuration, nil
}

func getQueryFloat64(url *url.URL, key string, defaultValue float64, logger ulogger.Logger) (float64, error) {
	value := url.Query().Get(key)
	if value == "" {
		logger.Infof("[Aerospike] %s=%f [default]", key, defaultValue)
		return defaultValue, nil
	}

	valueFloat64, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return defaultValue, errors.NewInvalidArgumentError("[Aerospike] Invalid value %s=%v", key, value, err)
	}

	logger.Infof("[Aerospike] %s=%f", key, valueFloat64)

	return valueFloat64, nil
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

// GetAerospikeReadPolicy creates a new Aerospike read policy with the provided options applied.
// Used to manage default connection parameters for read operations.
// If no options are provided, the policy will use the configured default values.
func GetAerospikeReadPolicy(tSettings *settings.Settings, options ...AerospikeReadPolicyOptions) *aerospike.BasePolicy {
	readPolicy := aerospike.NewPolicy()

	if tSettings.Aerospike.UseDefaultPolicies {
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

// WithExpiration sets the TTL (time-to-live) for records in seconds.
// Special values:
//   - aerospike.TTLServerDefault (0): Use namespace default-ttl
//   - aerospike.TTLDontExpire (MaxUint32): Never expire
//   - aerospike.TTLDontUpdate (MaxUint32-1): Don't change existing TTL
func WithExpiration(ttlSeconds uint32) AerospikeWritePolicyOptions {
	return func(policy *aerospike.WritePolicy) {
		policy.Expiration = ttlSeconds
	}
}

// GetAerospikeWritePolicy creates a new Aerospike write policy with the provided options applied.
// Used to manage default connection parameters for write operations with strong consistency.
// If no options are provided, the policy will use the configured default values.
func GetAerospikeWritePolicy(tSettings *settings.Settings, generation uint32, options ...AerospikeWritePolicyOptions) *aerospike.WritePolicy {
	writePolicy := aerospike.NewWritePolicy(generation, aerospike.TTLDontExpire)

	if tSettings.Aerospike.UseDefaultPolicies {
		return writePolicy
	}

	writePolicy.MaxRetries = writeMaxRetries
	writePolicy.TotalTimeout = writeTimeout
	writePolicy.SocketTimeout = writeSocketTimeout
	writePolicy.SleepBetweenRetries = writeSleepBetweenRetries
	writePolicy.SleepMultiplier = writeSleepMultiplier
	writePolicy.ExitFastOnExhaustedConnectionPool = writeExitFastOnExhaustedConnectionPool
	writePolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	// Apply the provided options
	for _, opt := range options {
		opt(writePolicy)
	}

	return writePolicy
}

// GetAerospikeBatchPolicy creates a new Aerospike batch policy configured with default settings.
// Used for batch read/write operations to optimize performance and consistency.
func GetAerospikeBatchPolicy(tSettings *settings.Settings) *aerospike.BatchPolicy {
	batchPolicy := aerospike.NewBatchPolicy()

	if tSettings.Aerospike.UseDefaultPolicies {
		return batchPolicy
	}

	batchPolicy.TotalTimeout = batchTotalTimeout
	batchPolicy.SocketTimeout = batchSocketTimeout
	batchPolicy.AllowInlineSSD = batchAllowInlineSSD
	batchPolicy.ConcurrentNodes = concurrentNodes
	batchPolicy.MaxRetries = batchMaxRetries
	batchPolicy.SleepBetweenRetries = batchSleepBetweenRetries
	batchPolicy.SleepMultiplier = batchSleepMultiplier

	return batchPolicy
}

// GetAerospikeBatchWritePolicy creates a new Aerospike batch write policy with strong consistency.
// Used for batch write operations to ensure data integrity across multiple records.
func GetAerospikeBatchWritePolicy(tSettings *settings.Settings) *aerospike.BatchWritePolicy {
	batchWritePolicy := aerospike.NewBatchWritePolicy()

	if tSettings.Aerospike.UseDefaultPolicies {
		return batchWritePolicy
	}

	batchWritePolicy.CommitLevel = aerospike.COMMIT_ALL // strong consistency

	return batchWritePolicy
}

// GetAerospikeBatchReadPolicy creates a new Aerospike batch read policy with default settings.
// Used for batch read operations to optimize throughput and consistency.
func GetAerospikeBatchReadPolicy(tSettings *settings.Settings) *aerospike.BatchReadPolicy {
	batchReadPolicy := aerospike.NewBatchReadPolicy()

	if tSettings.Aerospike.UseDefaultPolicies {
		return batchReadPolicy
	}

	return batchReadPolicy
}

// GetAerospikeQueryPolicy creates a new Aerospike query policy with configured settings.
// Used for query operations to scan and filter records.
func GetAerospikeQueryPolicy(tSettings *settings.Settings) *aerospike.QueryPolicy {
	queryPolicy := aerospike.NewQueryPolicy()

	if tSettings.Aerospike.UseDefaultPolicies {
		return queryPolicy
	}

	queryPolicy.MaxRetries = queryMaxRetries
	queryPolicy.TotalTimeout = queryTotalTimeout
	queryPolicy.SocketTimeout = querySocketTimeout
	queryPolicy.SleepBetweenRetries = querySleepBetweenRetries
	queryPolicy.SleepMultiplier = querySleepMultiplier

	return queryPolicy
}
