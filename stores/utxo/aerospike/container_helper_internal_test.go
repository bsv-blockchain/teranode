package aerospike

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// SharedTestNamespace and SharedTestSet are where every test on a shared server keeps its records.
// One set for all of them: the store creates its secondary indexes under fixed names, and an index
// name is unique within a namespace, so a store on a second set could not create its own.
const (
	SharedTestNamespace = "test"
	SharedTestSet       = "test"
)

// TestMain terminates the shared servers once every test has run. Nothing else would: CI disables
// the testcontainers reaper (TESTCONTAINERS_RYUK_DISABLED=true).
func TestMain(m *testing.M) {
	code := m.Run()

	terminateSharedTestServers()

	os.Exit(code)
}

// SharedTestServer is an Aerospike server that tests of this package share.
type SharedTestServer struct {
	Host string
	Port int
}

// sharedServerKey says which tests can share a server. utxostore_utxoBatchSize is part of it because
// the store records it in the namespace on first open and refuses a later open that disagrees
// (layout_marker.go).
type sharedServerKey struct {
	image     string
	platform  string
	batchSize int
	// generation is how many times this test ran before in this process (go test -count). A test
	// may build the same transactions every time it runs, and must not meet its own records.
	generation int
}

// testRuns remembers, per test name, the last run (its *testing.T) and that run's generation.
var (
	testRunsMu sync.Mutex
	testRuns   = map[string]testRun{}
)

type testRun struct {
	t          testing.TB
	generation int
}

// testGeneration returns 0 for a test's first run in this process, 1 for its second, and so on. A
// test that asks again in the same run gets the same generation.
func testGeneration(t testing.TB) int {
	testRunsMu.Lock()
	defer testRunsMu.Unlock()

	run, seen := testRuns[t.Name()]

	switch {
	case !seen:
		run = testRun{t: t}
	case run.t != t:
		run = testRun{t: t, generation: run.generation + 1}
	}

	testRuns[t.Name()] = run

	return run.generation
}

type sharedServer struct {
	once      sync.Once
	container *aeroTest.Container
	server    SharedTestServer
	err       error
}

var (
	sharedServersMu sync.Mutex
	sharedServers   = map[sharedServerKey]*sharedServer{}

	// containerStarts counts the containers this binary started, by kind, for the summary
	// TestMain prints.
	containerStarts sync.Map
)

// AcquireSharedTestServer returns the server that every test asking for the same image, platform and
// batch size shares, starting it on first use. A test run again in the same process (go test
// -count) gets the next set of servers, so it never meets its own records. An empty image means
// test.AerospikeImage. It skips the test when no container runtime is available and fails it when
// the server could not be started for any other reason.
//
// A test on a shared server must not depend on what else is in the namespace: its transactions must
// be its own (no other test creates them), it must not clear, scan or count a whole set, and it must
// not change the server (indexes, namespace configuration, UDF modules).
func AcquireSharedTestServer(t testing.TB, image, platform string, batchSize int) SharedTestServer {
	t.Helper()

	if image == "" {
		image = test.AerospikeImage
	}

	key := sharedServerKey{image: image, platform: platform, batchSize: batchSize, generation: testGeneration(t)}

	sharedServersMu.Lock()
	entry, ok := sharedServers[key]
	if !ok {
		entry = &sharedServer{}
		sharedServers[key] = entry
	}
	sharedServersMu.Unlock()

	entry.once.Do(func() {
		entry.container, entry.server, entry.err = startSharedTestServer(key)
	})

	// Skips only when the container runtime is unreachable; any other failure fails the test, so a
	// broken shared server cannot quietly skip every test that uses it.
	test.SkipIfContainerUnavailable(t, entry.err)

	return entry.server
}

// startSharedTestServer starts the container, waits until its namespace accepts writes and opens one
// store on it, so the UDF modules, the layout marker and the store's indexes exist before tests open
// stores of their own in parallel.
func startSharedTestServer(key sharedServerKey) (*aeroTest.Container, SharedTestServer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	opts := []testcontainers.ContainerCustomizer{
		aeroTest.WithTTLSupport(SharedTestNamespace),
		aeroTest.WithImage(key.image),
	}
	if key.platform != "" {
		opts = append(opts, testcontainers.WithImagePlatform(key.platform))
	}

	container, err := runContainerRecovering(ctx, opts...)
	if err != nil {
		return nil, SharedTestServer{}, err
	}

	CountContainerStart("shared", fmt.Sprintf("%s batch size %d generation %d", key.image, key.batchSize, key.generation))

	server, err := warmUpSharedTestServer(ctx, container, key.batchSize)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, SharedTestServer{}, err
	}

	return container, server, nil
}

func warmUpSharedTestServer(ctx context.Context, container *aeroTest.Container, batchSize int) (SharedTestServer, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return SharedTestServer{}, err
	}

	port, err := container.ServicePort(ctx)
	if err != nil {
		return SharedTestServer{}, err
	}

	client, aeroErr := uaerospike.NewClient(host, port)
	if aeroErr != nil {
		return SharedTestServer{}, aeroErr
	}
	defer client.Close()

	if err = WaitForWritesReady(client, SharedTestNamespace, SharedTestSet); err != nil {
		return SharedTestServer{}, err
	}

	tSettings := settings.NewSettings()
	tSettings.UtxoStore.UtxoBatchSize = batchSize

	storeURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s?set=%s&externalStore=file://./data/externalStore", host, port, SharedTestNamespace, SharedTestSet))
	if err != nil {
		return SharedTestServer{}, err
	}

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	if err != nil {
		return SharedTestServer{}, errors.NewProcessingError("could not open a store on the shared server", err)
	}

	closeErr := store.Close(ctx)
	if closeErr != nil {
		return SharedTestServer{}, closeErr
	}

	return SharedTestServer{Host: host, Port: port}, nil
}

// runContainerRecovering starts a container and turns a panic from testcontainers into an error.
func runContainerRecovering(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (container *aeroTest.Container, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewError("container startup panic: %v", r)
		}
	}()

	return aeroTest.RunContainer(ctx, opts...)
}

func terminateSharedTestServers() {
	sharedServersMu.Lock()
	defer sharedServersMu.Unlock()

	for key, entry := range sharedServers {
		if entry.container == nil {
			continue
		}

		// The client every store on this server shared (see LeaveSharedClientOpenOnClose).
		util.CloseAerospikeClient(fmt.Sprintf("%s:%d", entry.server.Host, entry.server.Port))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := entry.container.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "could not terminate the shared Aerospike container for %+v: %v\n", key, err)
		}
		cancel()
	}

	reportContainerStarts()
}

// CountContainerStart counts one container start of the given kind ("shared" or "own"), for the
// lines TestMain prints when AEROSPIKE_TEST_COUNT_CONTAINERS is set, and then also logs what the
// container is for.
func CountContainerStart(kind, what string) {
	if os.Getenv("AEROSPIKE_TEST_COUNT_CONTAINERS") != "" {
		fmt.Printf("aerospike test container start: %s for %s\n", kind, what)
	}

	counter, _ := containerStarts.LoadOrStore(kind, new(int64Counter))
	counter.(*int64Counter).add()
}

type int64Counter struct {
	mu sync.Mutex
	n  int64
}

func (c *int64Counter) add() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func reportContainerStarts() {
	if os.Getenv("AEROSPIKE_TEST_COUNT_CONTAINERS") == "" {
		return
	}

	// Peak resident memory of this test binary, on Linux.
	if status, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				fmt.Printf("aerospike test binary peak memory: %s\n", strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:")))
			}
		}
	}

	containerStarts.Range(func(kind, counter any) bool {
		c := counter.(*int64Counter)
		c.mu.Lock()
		fmt.Printf("aerospike test containers started: %s=%d\n", kind, c.n)
		c.mu.Unlock()

		return true
	})
}

// WaitForWritesReady polls the namespace until it accepts writes. Aerospike can answer
// FAIL_FORBIDDEN ("Operation not allowed at this time") for a few seconds after the service port
// opens, while the namespace finishes coming up.
func WaitForWritesReady(client *uaerospike.Client, namespace, set string) error {
	const (
		attempts = 60
		delay    = 200 * time.Millisecond
	)

	sentinelKey, err := aerospike.NewKey(namespace, set, []byte("__readiness_probe__"))
	if err != nil {
		return err
	}

	writePolicy := aerospike.NewWritePolicy(0, 0)
	writePolicy.RecordExistsAction = aerospike.REPLACE

	var lastErr error

	for i := 0; i < attempts; i++ {
		putErr := client.Put(writePolicy, sentinelKey, aerospike.BinMap{"ready": true})
		if putErr == nil {
			_, _ = client.Delete(writePolicy, sentinelKey)
			return nil
		}

		if putErr.Matches(types.FAIL_FORBIDDEN) {
			lastErr = putErr
			time.Sleep(delay)

			continue
		}

		return putErr
	}

	return errors.NewProcessingError("aerospike namespace %q did not become write-ready after %v", namespace, time.Duration(attempts)*delay, lastErr)
}

// acquireSharedTestServerForInternalTests is AcquireSharedTestServer with the image the internal
// tests run on (see runAerospikeTestContainer).
func acquireSharedTestServerForInternalTests(t *testing.T, batchSize int) SharedTestServer {
	t.Helper()

	image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE")

	platform := ""
	if image != "" {
		platform = cmp.Or(os.Getenv("AEROSPIKE_CONTAINER_PLATFORM"), "linux/amd64")
	}

	return AcquireSharedTestServer(t, image, platform, batchSize)
}

// newSharedTestStore opens a store on the shared server, with query appended to the store URL,
// and a raw client to it; both close when the test ends.
func newSharedTestStore(t *testing.T, logger ulogger.Logger, tSettings *settings.Settings, query string) (*Store, *uaerospike.Client) {
	t.Helper()

	server := acquireSharedTestServerForInternalTests(t, tSettings.UtxoStore.UtxoBatchSize)

	client, aeroErr := uaerospike.NewClient(server.Host, server.Port)
	require.NoError(t, aeroErr)
	t.Cleanup(client.Close)

	storeURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s?set=%s%s", server.Host, server.Port, SharedTestNamespace, SharedTestSet, query))
	require.NoError(t, err)

	store, err := New(context.Background(), logger, tSettings, storeURL)
	require.NoError(t, err)

	store.LeaveSharedClientOpenOnClose()
	t.Cleanup(store.StopConflictingIndexSelfTest)

	return store, client
}

// ownServerSlots bounds how many tests run on a server of their own at once: each one is a
// container, and the parallel tests already share the machine with the shared servers.
var ownServerSlots = make(chan struct{}, 8)

// AcquireOwnServerSlot waits for one of the slots for a server of a test's own and gives it back
// when the test ends.
func AcquireOwnServerSlot(t testing.TB) {
	t.Helper()

	ownServerSlots <- struct{}{}
	t.Cleanup(func() { <-ownServerSlots })
}

// uniqueLockTime is the last lock time NextUniqueLockTime handed out.
var uniqueLockTime atomic.Uint32

// NextUniqueLockTime returns a lock time no earlier call returned. A transaction given one has a
// txid of its own, so tests that share a server never create the same transaction; the store does
// not interpret the lock time.
func NextUniqueLockTime() uint32 {
	return uniqueLockTime.Add(1)
}

// runAerospikeTestContainer starts an Aerospike server for a test: test.AerospikeImage, or the
// image in AEROSPIKE_CONTAINER_IMAGE (for example the mod-teranode fork) on the platform in
// AEROSPIKE_CONTAINER_PLATFORM, linux/amd64 when that is unset.
func runAerospikeTestContainer(t testing.TB, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*aeroTest.Container, error) {
	AcquireOwnServerSlot(t)
	CountContainerStart("own", t.Name())

	image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE")
	if image == "" {
		return aeroTest.RunContainer(ctx, append(opts, aeroTest.WithImage(test.AerospikeImage))...)
	}

	opts = append(opts, aeroTest.WithImage(image))

	platform := os.Getenv("AEROSPIKE_CONTAINER_PLATFORM")
	if platform == "" {
		platform = "linux/amd64"
	}
	opts = append(opts, testcontainers.WithImagePlatform(platform))

	return aeroTest.RunContainer(ctx, opts...)
}
