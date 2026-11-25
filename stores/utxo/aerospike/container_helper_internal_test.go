package aerospike

import (
	"cmp"
	"context"
	"flag"
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

// TestMain terminates the shared servers and the pool servers once every test has run. Nothing
// else would: CI disables the testcontainers reaper (TESTCONTAINERS_RYUK_DISABLED=true).
func TestMain(m *testing.M) {
	flag.Parse()

	// A full run uses every pool server and four shared ones; start them while the first tests,
	// which need none, run.
	if flag.Lookup("test.run").Value.String() == "" {
		prestartServers(ownServerPoolSize + 4)
	}

	code := m.Run()

	var wg sync.WaitGroup

	wg.Go(terminateSharedTestServers)
	wg.Go(terminateOwnServers)
	wg.Go(terminateUnusedPrestartedServers)
	wg.Wait()

	os.Exit(code)
}

// prestarted holds the containers TestMain starts before the tests run; prestartedUnclaimed counts
// those no test has taken yet.
var (
	prestartedKey       sharedServerKey
	prestarted          chan *aeroTest.Container
	prestartedUnclaimed atomic.Int32
)

func prestartServers(count int) {
	image, platform := internalTestImage()
	prestartedKey = sharedServerKey{image: cmp.Or(image, test.AerospikeImage), platform: platform}
	prestarted = make(chan *aeroTest.Container, count)
	prestartedUnclaimed.Store(int32(count))

	for range count {
		go func() {
			container, err := runServerContainer(context.Background(), prestartedKey)
			if err != nil {
				container = nil // the test that takes it starts one itself
			}

			prestarted <- container
		}()
	}
}

// startServerContainer returns a prestarted container when one is left for the key, and starts one
// otherwise.
func startServerContainer(ctx context.Context, key sharedServerKey) (*aeroTest.Container, error) {
	if prestarted != nil && key.image == prestartedKey.image && key.platform == prestartedKey.platform && prestartedUnclaimed.Add(-1) >= 0 {
		if container := <-prestarted; container != nil {
			return container, nil
		}
	}

	return runServerContainer(ctx, key)
}

// runServerContainer starts a container for the key's image and platform, turning a panic from
// testcontainers into an error.
func runServerContainer(ctx context.Context, key sharedServerKey) (container *aeroTest.Container, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewError("container startup panic: %v", r)
		}
	}()

	opts := []testcontainers.ContainerCustomizer{
		aeroTest.WithTTLSupport(SharedTestNamespace),
		aeroTest.WithImage(key.image),
	}
	if key.platform != "" {
		opts = append(opts, testcontainers.WithImagePlatform(key.platform))
	}

	return aeroTest.RunContainer(ctx, opts...)
}

func terminateUnusedPrestartedServers() {
	if prestarted == nil {
		return
	}

	for unclaimed := prestartedUnclaimed.Load(); unclaimed > 0; unclaimed-- {
		if container := <-prestarted; container != nil {
			_ = container.Terminate(context.Background())
		}
	}
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
		entry.container, entry.server, entry.err = startSharedTestServer(t, key)
	})

	// Skips only when the container runtime is unreachable; any other failure fails the test, so a
	// broken shared server cannot quietly skip every test that uses it.
	test.SkipIfContainerUnavailable(t, entry.err)

	return entry.server
}

// startSharedTestServer starts the container, waits until its namespace accepts writes and opens one
// store on it, so the UDF modules, the layout marker and the store's indexes exist before tests open
// stores of their own in parallel.
func startSharedTestServer(t testing.TB, key sharedServerKey) (*aeroTest.Container, SharedTestServer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := startServerContainer(ctx, key)
	if err != nil {
		return nil, SharedTestServer{}, err
	}

	server, err := warmUpSharedTestServer(ctx, t, container, key.batchSize)
	if err != nil {
		_ = container.Terminate(context.Background())
		return nil, SharedTestServer{}, err
	}

	return container, server, nil
}

func warmUpSharedTestServer(ctx context.Context, t testing.TB, container *aeroTest.Container, batchSize int) (SharedTestServer, error) {
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

	// The tests' own settings: creating the store's client publishes its policy values to every
	// store in the process (util.GetAerospikeClient), so they must be the ones the tests use.
	tSettings := test.CreateBaseTestSettings(t)
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

func terminateSharedTestServers() {
	sharedServersMu.Lock()
	defer sharedServersMu.Unlock()

	var wg sync.WaitGroup

	for key, entry := range sharedServers {
		if entry.container == nil {
			continue
		}

		// The client every store on this server shared (see LeaveSharedClientOpenOnClose).
		util.CloseAerospikeClient(fmt.Sprintf("%s:%d", entry.server.Host, entry.server.Port))

		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := entry.container.Terminate(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "could not terminate the shared Aerospike container for %+v: %v\n", key, err)
			}
		})
	}

	wg.Wait()

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

// acquireSharedTestServerForInternalTests is AcquireSharedTestServer on the internal tests' image.
func acquireSharedTestServerForInternalTests(t *testing.T, batchSize int) SharedTestServer {
	t.Helper()

	image, platform := internalTestImage()

	return AcquireSharedTestServer(t, image, platform, batchSize)
}

// internalTestImage is the image the internal tests run on: test.AerospikeImage, or the image in
// AEROSPIKE_CONTAINER_IMAGE (for example the mod-teranode fork) on the platform in
// AEROSPIKE_CONTAINER_PLATFORM, linux/amd64 when that is unset.
func internalTestImage() (image, platform string) {
	image = os.Getenv("AEROSPIKE_CONTAINER_IMAGE")
	if image != "" {
		platform = cmp.Or(os.Getenv("AEROSPIKE_CONTAINER_PLATFORM"), "linux/amd64")
	}

	return image, platform
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

// uniqueLockTime is the last lock time NextUniqueLockTime handed out.
var uniqueLockTime atomic.Uint32

// NextUniqueLockTime returns a lock time no earlier call returned. A transaction given one has a
// txid of its own, so tests that share a server never create the same transaction; the store does
// not interpret the lock time.
func NextUniqueLockTime() uint32 {
	return uniqueLockTime.Add(1)
}

// ownServerPoolSize bounds how many servers the tests that need a server to themselves use at once.
// The servers are reused: each test gets one to itself, and it is wiped before the next test gets it.
const ownServerPoolSize = 8

// ownServer is a server of the pool. retire says it is terminated, not reused, when its test ends.
type ownServer struct {
	key       sharedServerKey // image and platform only
	container *aeroTest.Container
	server    SharedTestServer
	retire    bool

	// What a store leaves on a new server: the wipe removes any other index or UDF module.
	indexes map[namespaceIndex]bool
	udfs    map[string]string // file name to hash
}

var (
	ownServersMu   sync.Mutex
	ownServersFree = sync.NewCond(&ownServersMu)
	ownServersIdle []*ownServer
	ownServersBusy = map[testing.TB]*ownServer{}
	ownServersLive = map[sharedServerKey]int{} // started and not terminated, per image
)

// AcquireOwnTestServer gives the test a server no other test uses until it ends. The server holds no
// records, and only the secondary indexes and UDF modules a store creates when it opens on a new
// server. An empty image means test.AerospikeImage. It skips the test when no container runtime is
// available.
func AcquireOwnTestServer(t testing.TB, image, platform string) SharedTestServer {
	t.Helper()

	key := sharedServerKey{image: cmp.Or(image, test.AerospikeImage), platform: platform}

	ownServersMu.Lock()

	var srv *ownServer

	for srv == nil {
		for i, idle := range ownServersIdle {
			if idle.key == key {
				srv = idle
				ownServersIdle = append(ownServersIdle[:i], ownServersIdle[i+1:]...)

				break
			}
		}

		switch {
		case srv != nil:
		case ownServersLive[key] < ownServerPoolSize:
			srv = &ownServer{key: key}
			ownServersLive[key]++
		default:
			ownServersFree.Wait()
		}
	}

	ownServersMu.Unlock()

	if srv.container == nil {
		if err := startOwnServer(t, srv); err != nil {
			ownServersMu.Lock()
			ownServersLive[key]--
			ownServersFree.Broadcast()
			ownServersMu.Unlock()

			test.SkipIfContainerUnavailable(t, err)
		}
	}

	ownServersMu.Lock()
	ownServersBusy[t] = srv
	ownServersMu.Unlock()

	t.Cleanup(func() { releaseOwnServer(t, srv) })

	return srv.server
}

// RetireOwnTestServer has the pool terminate the test's server when the test ends, instead of wiping
// it for the next test. It is for a test that changes what the wipe does not reset, such as the
// namespace configuration.
func RetireOwnTestServer(t testing.TB) {
	ownServersMu.Lock()
	defer ownServersMu.Unlock()

	if srv, ok := ownServersBusy[t]; ok {
		srv.retire = true
	}
}

// startOwnServer starts a server as a shared one is started, records which indexes and UDF modules
// the store opened on it left behind, and deletes the records that store wrote.
func startOwnServer(t testing.TB, srv *ownServer) error {
	key := srv.key
	key.batchSize = test.CreateBaseTestSettings(t).UtxoStore.UtxoBatchSize

	var err error
	if srv.container, srv.server, err = startSharedTestServer(t, key); err != nil {
		return err
	}

	if err = srv.snapshot(); err == nil {
		err = srv.wipe()
	}

	if err != nil {
		terminateOwnServer(srv)
	}

	return err
}

// releaseOwnServer wipes the server and gives it back to the pool, or terminates it if it is
// retired or the wipe fails.
func releaseOwnServer(t testing.TB, srv *ownServer) {
	addr := fmt.Sprintf("%s:%d", srv.server.Host, srv.server.Port)

	// Close the client the test's stores shared (util caches one per host), so a store the test
	// left open cannot write into the next test's namespace.
	util.CloseAerospikeClient(addr)

	ownServersMu.Lock()
	delete(ownServersBusy, t)
	retire := srv.retire
	ownServersMu.Unlock()

	var err error
	if !retire {
		err = srv.wipe()
	}

	if retire || err != nil {
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not wipe the Aerospike server at %s, terminating it: %v\n", addr, err)
		}

		terminateOwnServer(srv)

		ownServersMu.Lock()
		ownServersLive[srv.key]--
		ownServersFree.Broadcast()
		ownServersMu.Unlock()

		return
	}

	ownServersMu.Lock()
	ownServersIdle = append(ownServersIdle, srv)
	ownServersFree.Broadcast()
	ownServersMu.Unlock()
}

// withInfo runs fn with a client to the server and a function sending it info commands.
func (srv *ownServer) withInfo(fn func(client *uaerospike.Client, info func(string) (string, error)) error) error {
	client, err := uaerospike.NewClient(srv.server.Host, srv.server.Port)
	if err != nil {
		return err
	}
	defer client.Close()

	nodes := client.GetNodes()
	if len(nodes) != 1 {
		return errors.NewProcessingError("expected one node, found %d", len(nodes))
	}

	return fn(client, func(command string) (string, error) {
		answers, infoErr := nodes[0].RequestInfo(aerospike.NewInfoPolicy(), command)
		if infoErr != nil {
			return "", infoErr
		}

		return answers[command], nil
	})
}

// snapshot records the server's indexes and UDF modules as the state the wipe returns it to.
func (srv *ownServer) snapshot() error {
	return srv.withInfo(func(client *uaerospike.Client, info func(string) (string, error)) error {
		indexes, err := namespaceIndexes(info)
		if err != nil {
			return err
		}

		udfs, err := client.ListUDF(nil)
		if err != nil {
			return err
		}

		srv.indexes = map[namespaceIndex]bool{}
		for _, index := range indexes {
			srv.indexes[index] = true
		}

		srv.udfs = map[string]string{}
		for _, udf := range udfs {
			srv.udfs[udf.Filename] = udf.Hash
		}

		return nil
	})
}

// wipe deletes every record and every index or UDF module the snapshot does not have. An index or
// module a test removed is left for the next store to create again.
func (srv *ownServer) wipe() error {
	return srv.withInfo(func(client *uaerospike.Client, info func(string) (string, error)) error {
		truncate := "truncate-namespace:namespace=" + SharedTestNamespace
		if answer, err := info(truncate); err != nil || answer != "ok" {
			return errors.NewProcessingError("%s answered %q", truncate, answer, err)
		}

		indexes, err := namespaceIndexes(info)
		if err != nil {
			return err
		}

		for _, index := range indexes {
			if !srv.indexes[index] {
				if err = client.DropIndex(nil, SharedTestNamespace, index.set, index.name); err != nil {
					return err
				}
			}
		}

		udfs, err := client.ListUDF(nil)
		if err != nil {
			return err
		}

		for _, udf := range udfs {
			if srv.udfs[udf.Filename] == udf.Hash {
				continue
			}

			task, removeErr := client.RemoveUDF(nil, udf.Filename)
			if removeErr != nil {
				return removeErr
			}

			if removeErr = <-task.OnComplete(); removeErr != nil {
				return removeErr
			}
		}

		// The server deletes truncated records in the background, but stops returning them at once.
		recordset, err := client.ScanAll(aerospike.NewScanPolicy(), SharedTestNamespace, "")
		if err != nil {
			return err
		}

		left := 0
		for result := range recordset.Results() {
			if result.Err != nil {
				return result.Err
			}

			left++
		}

		if left != 0 {
			return errors.NewProcessingError("%d records left after truncating the namespace", left)
		}

		return nil
	})
}

// namespaceIndex is a secondary index; definition is its sindex-list entry without its build state,
// so an index of the same name over another bin or expression counts as another index.
type namespaceIndex struct{ name, set, definition string }

// namespaceIndexes lists the secondary indexes of the test namespace, from sindex-list entries such
// as "ns=test:indexname=unminedSinceIndex:set=test:bin=unminedSince:...".
func namespaceIndexes(info func(string) (string, error)) ([]namespaceIndex, error) {
	answer, err := info("sindex-list")
	if err != nil {
		return nil, err
	}

	var indexes []namespaceIndex

	for _, entry := range strings.Split(answer, ";") {
		fields := map[string]string{}

		var definition []string

		for _, field := range strings.Split(entry, ":") {
			if name, value, ok := strings.Cut(field, "="); ok {
				fields[name] = value

				if name != "state" {
					definition = append(definition, field)
				}
			}
		}

		if cmp.Or(fields["ns"], fields["namespace"]) != SharedTestNamespace || fields["indexname"] == "" {
			continue
		}

		set := fields["set"]
		if set == "NULL" {
			set = ""
		}

		indexes = append(indexes, namespaceIndex{name: fields["indexname"], set: set, definition: strings.Join(definition, ":")})
	}

	return indexes, nil
}

func terminateOwnServer(srv *ownServer) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.container.Terminate(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "could not terminate an Aerospike test container: %v\n", err)
	}
}

// terminateOwnServers terminates the pool's idle servers once every test has run.
func terminateOwnServers() {
	ownServersMu.Lock()
	idle := ownServersIdle
	ownServersIdle = nil
	ownServersMu.Unlock()

	var wg sync.WaitGroup

	for _, srv := range idle {
		wg.Go(func() { terminateOwnServer(srv) })
	}

	wg.Wait()
}

// acquireOwnTestServerForInternalTests is AcquireOwnTestServer on the internal tests' image.
func acquireOwnTestServerForInternalTests(t testing.TB) SharedTestServer {
	t.Helper()

	image, platform := internalTestImage()

	return AcquireOwnTestServer(t, image, platform)
}
