package collect

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	aero "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
)

const (
	aerospikeDefaultPort   = 3000
	aerospikeBytesPerGB    = 1e9
	aerospikeP99PercentCap = 1.0
	aerospikeLatencyNote   = "latency not exposed"
	aerospikeLatencyWindow = "30"
)

type Aerospike struct {
	hosts     []*aero.Host
	namespace string
	user      string
	password  string
	timeout   time.Duration

	readFn func() (aerospikeReadings, error)

	mu     sync.Mutex
	client *aero.Client
	closed bool

	panicMu sync.Mutex
	pending any
}

type aerospikeReadings struct {
	size snapshot.Reading
	fill snapshot.Reading
	p99  snapshot.Reading
}

func NewAerospike(u *url.URL, timeout time.Duration) (*Aerospike, error) {
	if u == nil {
		return &Aerospike{timeout: timeout}, nil
	}

	if timeout <= 0 {
		return nil, errors.NewConfigurationError("probe aerospike collector needs a positive timeout")
	}

	namespace := strings.Trim(u.Path, "/")
	if namespace == "" {
		return nil, errors.NewConfigurationError("probe aerospike url has no namespace")
	}

	if i := strings.Index(namespace, "/"); i >= 0 {
		namespace = namespace[:i]
	}

	hosts, err := aerospikeHosts(u.Host)
	if err != nil {
		return nil, err
	}

	a := &Aerospike{hosts: hosts, namespace: namespace, timeout: timeout}
	a.readFn = a.read

	if u.User != nil {
		if password, ok := u.User.Password(); ok {
			a.user = u.User.Username()
			a.password = password
		}
	}

	return a, nil
}

func aerospikeHosts(hostList string) ([]*aero.Host, error) {
	var hosts []*aero.Host

	for _, host := range strings.Split(hostList, ",") {
		parts := strings.Split(host, ":")

		switch len(parts) {
		case 1:
			if parts[0] == "" {
				return nil, errors.NewConfigurationError("probe aerospike url has an empty host")
			}

			hosts = append(hosts, aero.NewHost(parts[0], aerospikeDefaultPort))
		case 2:
			port, err := strconv.Atoi(parts[1])
			if err != nil || parts[0] == "" {
				return nil, errors.NewConfigurationError("probe aerospike url has an invalid host %s", host)
			}

			hosts = append(hosts, aero.NewHost(parts[0], port))
		default:
			return nil, errors.NewConfigurationError("probe aerospike url has an invalid host %s", host)
		}
	}

	if len(hosts) == 0 {
		return nil, errors.NewConfigurationError("probe aerospike url has no host")
	}

	return hosts, nil
}

func AerospikeURL(s *settings.Settings) *url.URL {
	if s == nil {
		return nil
	}

	if s.Probe.AerospikeURL != nil {
		return s.Probe.AerospikeURL
	}

	if u := s.UtxoStore.UtxoStore; u != nil && u.Scheme == "aerospike" {
		return u
	}

	return nil
}

func (a *Aerospike) Name() string {
	return "aerospike"
}

func (a *Aerospike) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.closed = true

	if a.client != nil {
		a.client.Close()
		a.client = nil
	}
}

func (a *Aerospike) Collect(ctx context.Context) (snapshot.Patch, error) {
	if len(a.hosts) == 0 {
		return notConfiguredAerospikePatch, nil
	}

	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	if v := a.takePendingPanic(); v != nil {
		panic(fmt.Sprintf("aerospike read panicked: %v", v))
	}

	type result struct {
		readings aerospikeReadings
		err      error
		panicked bool
	}

	done := make(chan result, 1)
	read := a.readFn

	go func() {
		defer guard.Recover(guard.RecorderFunc(func(_ string, v any) {
			a.setPendingPanic(v)
			done <- result{panicked: true}
		}), "aerospike read")

		readings, err := read()
		done <- result{readings: readings, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.NewServiceError("aerospike info request did not finish in %s", a.timeout, ctx.Err())
	case res := <-done:
		if res.panicked {
			panic(fmt.Sprintf("aerospike read panicked: %v", a.takePendingPanic()))
		}

		if res.err != nil {
			return nil, res.err
		}

		readings := res.readings

		return func(s *snapshot.Snapshot) {
			st := &s.Stores.Aerospike
			st.Configured = true
			st.SizeGB = readings.size
			st.DiskFill = readings.fill
			st.ReadP99Sec = readings.p99
		}, nil
	}
}

func (a *Aerospike) setPendingPanic(v any) {
	a.panicMu.Lock()
	defer a.panicMu.Unlock()

	a.pending = v
}

func (a *Aerospike) takePendingPanic() any {
	a.panicMu.Lock()
	defer a.panicMu.Unlock()

	v := a.pending
	a.pending = nil

	return v
}

func (a *Aerospike) hasPendingPanic() bool {
	a.panicMu.Lock()
	defer a.panicMu.Unlock()

	return a.pending != nil
}

func notConfiguredAerospikePatch(s *snapshot.Snapshot) {
	st := &s.Stores.Aerospike
	st.Configured = false
	st.SizeGB = snapshot.Reading{Status: snapshot.StatusNoData}
	st.DiskFill = snapshot.Reading{Status: snapshot.StatusNoData}
	st.ReadP99Sec = snapshot.Reading{Status: snapshot.StatusNoData}
}

func (a *Aerospike) connection() (*aero.Client, error) {
	a.mu.Lock()
	client, closed := a.client, a.closed
	a.mu.Unlock()

	if closed {
		return nil, errors.NewServiceError("probe aerospike collector is closed")
	}

	if client != nil {
		return client, nil
	}

	policy := aero.NewClientPolicy()
	policy.Timeout = a.timeout
	policy.LoginTimeout = a.timeout
	policy.FailIfNotConnected = true

	if a.user != "" {
		policy.AuthMode = aero.AuthModeInternal
		policy.User = a.user
		policy.Password = a.password
	}

	created, err := aero.NewClientWithPolicyAndHost(policy, a.hosts...)
	if err != nil {
		return nil, errors.NewServiceError("could not connect probe to aerospike", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		created.Close()
		return nil, errors.NewServiceError("probe aerospike collector is closed")
	}

	if a.client != nil {
		created.Close()
		return a.client, nil
	}

	a.client = created

	return created, nil
}

func (a *Aerospike) dropClient(client *aero.Client) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.client == client {
		a.client = nil
	}

	client.Close()
}

type nodeStats struct {
	name     string
	answered bool
	usage    bool
	sizeGB   float64
	fill     float64
	p99      float64
	p99ok    bool
}

func aggregateNodes(nodes []nodeStats) (sizeGB float64, fill float64, p99 float64, p99ok bool, ok bool) {
	for _, n := range nodes {
		if !n.answered {
			continue
		}

		if n.usage {
			ok = true
			sizeGB += n.sizeGB
			fill = max(fill, n.fill)
		}

		if n.p99ok {
			p99ok = true
			p99 = max(p99, n.p99)
		}
	}

	return sizeGB, fill, p99, p99ok, ok
}

func (a *Aerospike) nodeInfo(node *aero.Node) nodeStats {
	stats := nodeStats{name: node.GetName()}

	policy := aero.NewInfoPolicy()
	policy.Timeout = a.timeout

	latency := readLatencyCommand(a.namespace)

	info, err := node.RequestInfo(policy, "namespace/"+a.namespace, latency)
	if err != nil {
		return stats
	}

	ns, found := info["namespace/"+a.namespace]
	if !found || strings.HasPrefix(ns, "ERROR") {
		return stats
	}

	stats.answered = true

	if used, fill, ok := namespaceUsage(ns); ok {
		stats.usage = true
		stats.sizeGB = used / aerospikeBytesPerGB
		stats.fill = fill
	}

	stats.p99, stats.p99ok = parseReadLatencyP99(info[latency], a.namespace)

	return stats
}

func (a *Aerospike) read() (aerospikeReadings, error) {
	client, err := a.connection()
	if err != nil {
		return aerospikeReadings{}, err
	}

	nodes := client.GetNodes()
	if len(nodes) == 0 {
		a.dropClient(client)
		return aerospikeReadings{}, errors.NewServiceError("probe aerospike client has no active node")
	}

	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetName() < nodes[j].GetName() })

	type nodeResult struct {
		index    int
		stats    nodeStats
		panicked any
	}

	out := make(chan nodeResult, len(nodes))

	for i, node := range nodes {
		go func() {
			defer guard.Recover(guard.RecorderFunc(func(_ string, v any) { out <- nodeResult{index: i, panicked: v} }), "aerospike node info")

			out <- nodeResult{index: i, stats: a.nodeInfo(node)}
		}()
	}

	results := make([]nodeStats, len(nodes))

	var panicked any

	for range nodes {
		r := <-out
		results[r.index] = r.stats

		if r.panicked != nil {
			panicked = r.panicked
		}
	}

	if panicked != nil {
		panic(panicked)
	}

	answered := 0

	for _, n := range results {
		if n.answered {
			answered++
		}
	}

	if answered == 0 {
		a.dropClient(client)
		return aerospikeReadings{}, errors.NewServiceError("no aerospike node returned statistics for namespace %s", a.namespace)
	}

	readings := aerospikeReadings{
		size: snapshot.Reading{Status: snapshot.StatusNoData},
		fill: snapshot.Reading{Status: snapshot.StatusNoData},
		p99:  snapshot.Reading{Status: snapshot.StatusNoData, Note: aerospikeLatencyNote},
	}

	sizeGB, fill, p99, p99ok, ok := aggregateNodes(results)

	if ok {
		readings.size = snapshot.Reading{Value: sizeGB, Status: snapshot.StatusOK}
		readings.fill = snapshot.Reading{Value: fill, Status: snapshot.StatusOK}
	}

	if p99ok {
		readings.p99 = snapshot.Reading{Value: p99, Status: snapshot.StatusOK}
	}

	return readings, nil
}

func statsMap(info string) map[string]string {
	out := make(map[string]string)

	for _, pair := range strings.Split(strings.TrimSpace(info), ";") {
		key, value, found := strings.Cut(pair, "=")
		if found {
			out[key] = value
		}
	}

	return out
}

func namespaceUsage(info string) (used float64, fill float64, ok bool) {
	stats := statsMap(info)

	candidates := [][2]string{
		{"data_used_bytes", "data_total_bytes"},
		{"device_used_bytes", "device_total_bytes"},
		{"memory_used_bytes", "memory-size"},
	}

	for _, c := range candidates {
		u, uerr := strconv.ParseFloat(stats[c[0]], 64)
		total, terr := strconv.ParseFloat(stats[c[1]], 64)

		if uerr != nil || terr != nil || total <= 0 {
			continue
		}

		return u, min(u/total, 1), true
	}

	return 0, 0, false
}

func readLatencyCommand(ns string) string {
	return "latencies:hist={" + ns + "}-read;back=" + aerospikeLatencyWindow + ";duration=" + aerospikeLatencyWindow
}

func latencyUnitScale(unit string) (float64, bool) {
	switch strings.TrimSpace(unit) {
	case "msec":
		return 1e-3, true
	case "usec":
		return 1e-6, true
	case "sec":
		return 1, true
	}

	return 0, false
}

func parseLatencyThreshold(label string) (float64, bool) {
	label = strings.TrimPrefix(strings.TrimSpace(label), ">")

	for _, unit := range []struct {
		suffix string
		scale  float64
	}{{"ms", 1e-3}, {"us", 1e-6}, {"s", 1}} {
		if num, found := strings.CutSuffix(label, unit.suffix); found {
			v, err := strconv.ParseFloat(num, 64)
			if err != nil || v <= 0 {
				return 0, false
			}

			return v * unit.scale, true
		}
	}

	return 0, false
}

func pickP99(ops string, thresholds []float64, percents []string) (float64, bool) {
	if len(thresholds) == 0 || len(thresholds) != len(percents) {
		return 0, false
	}

	opsPerSec, err := strconv.ParseFloat(strings.TrimSpace(ops), 64)
	if err != nil || opsPerSec <= 0 {
		return 0, false
	}

	for i, seconds := range thresholds {
		percent, perr := strconv.ParseFloat(strings.TrimSpace(percents[i]), 64)
		if perr != nil {
			return 0, false
		}

		if percent < aerospikeP99PercentCap {
			return seconds, true
		}
	}

	return thresholds[len(thresholds)-1], true
}

func parseReadLatencyP99(info string, ns string) (float64, bool) {
	prefix := "{" + ns + "}-read:"
	segments := strings.Split(info, ";")

	for i, segment := range segments {
		segment = strings.TrimSpace(segment)
		if !strings.HasPrefix(segment, prefix) {
			continue
		}

		fields := strings.Split(strings.TrimPrefix(segment, prefix), ",")
		if len(fields) < 3 {
			return 0, false
		}

		if scale, ok := latencyUnitScale(fields[0]); ok {
			thresholds := make([]float64, len(fields)-2)
			for j := range thresholds {
				thresholds[j] = scale * float64(uint64(1)<<uint(j))
			}

			return pickP99(fields[1], thresholds, fields[2:])
		}

		if i+1 >= len(segments) {
			return 0, false
		}

		data := strings.Split(strings.TrimSpace(segments[i+1]), ",")
		if len(data) != len(fields) {
			return 0, false
		}

		thresholds := make([]float64, 0, len(fields)-2)

		for _, label := range fields[2:] {
			seconds, ok := parseLatencyThreshold(label)
			if !ok {
				return 0, false
			}

			thresholds = append(thresholds, seconds)
		}

		return pickP99(data[1], thresholds, data[2:])
	}

	return 0, false
}
