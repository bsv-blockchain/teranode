package main

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/propagation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"golang.org/x/sync/errgroup"
)

// dummyT satisfies test.TestingT so CreateBaseTestSettings can be called from
// outside a *testing.T context. Logs go to stderr; TempDir creates a real
// temporary directory that is removed when the fixture is cleaned up.
type dummyT struct {
	tmpDir string
}

func newDummyT() *dummyT {
	dir, err := os.MkdirTemp("", "validator-loadtest-*")
	if err != nil {
		log.Fatalf("fixture: mktemp: %v", err)
	}
	return &dummyT{tmpDir: dir}
}

func (d *dummyT) Errorf(format string, args ...interface{}) {
	log.Printf("[test] ERROR: "+format, args...)
}

func (d *dummyT) Logf(format string, args ...interface{}) {
	log.Printf("[test] "+format, args...)
}

func (d *dummyT) TempDir() string {
	return d.tmpDir
}

func (d *dummyT) cleanup() {
	_ = os.RemoveAll(d.tmpDir)
}

// fixture is the wiring the harness needs: a real Aerospike (via
// testcontainers) backing a real Validator wired into a PropagationServer
// with the coalescer constructed.
type fixture struct {
	ps            *propagation.PropagationServer
	aeroStore     *aerostore.Store
	parents       []*bt.Tx
	cleanup       func()
	containerName string               // empty when using --aerospike-url; used by the telemetry sampler
	validator     *validator.Validator // for PhaseSnapshot access; nil if type-assert fails
}

// tunedAerospikeConf is a static aerospike.conf injected into the testcontainer
// to remove the default service-thread ceiling.
// It is mounted at /etc/aerospike/aerospike.template.conf so the container
// entrypoint picks it up (the entrypoint copies the template → aerospike.conf
// before starting asd). Lines without $() or {} pass through the template
// processor unchanged, so a static config is safe here.
//
// Storage engine choice: device (file-backed) with read-page-cache enabled.
// Benchmarking showed storage-engine memory is ~43% slower than device+file
// for the Lua-heavy spend path — Aerospike 8's in-memory allocator appears
// to have higher lock contention than the device engine's warm page cache.
//
// Tuning rationale (for the EPYC 9554 / 125 GiB Hetzner box):
//   - service-threads 64      : default is ~5; raise to match CPU count
//   - batch-index-threads 64  : parallel batch-index processing
//   - proto-fd-max 50000      : default 15000; headroom for 1024 submitters
//     (requires nofile ulimit >= 50000; see withTunedAerospikeConfig)
//   - filesize 8G             : room for 1.5M parents + all child UTXOs
//   - read-page-cache true    : keep hot data in OS page cache (fast)
const tunedAerospikeConf = `# aerospike.conf tuned for loadtest — static file, no shell substitution
service {
	cluster-name docker
	service-threads 64
	batch-index-threads 64
	proto-fd-max 50000
}

logging {
	console {
		context any info
	}
}

network {
	service {
		address any
		port 3000
	}

	heartbeat {
		mode mesh
		address local
		port 3002
		interval 150
		timeout 10
	}

	fabric {
		address local
		port 3001
	}
}

namespace test {
	replication-factor 1
	storage-engine device {
		file /opt/aerospike/data/test.dat
		filesize 8G
		read-page-cache true
	}
}
`

// withTunedAerospikeConfig injects the tuned aerospike.conf into the container
// by replacing the default template before the entrypoint processes it, and
// raises the nofile ulimit so proto-fd-max 50000 doesn't get rejected.
func withTunedAerospikeConfig() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		// Raise the nofile ulimit so Aerospike accepts proto-fd-max 50000.
		// The default Docker container ulimit (20480 on some hosts) would
		// cause asd to abort at startup. 65535 is within Docker's soft cap.
		hostCfgFn := req.HostConfigModifier
		req.HostConfigModifier = func(hc *container.HostConfig) {
			if hostCfgFn != nil {
				hostCfgFn(hc)
			}
			hc.Ulimits = append(hc.Ulimits, &container.Ulimit{
				Name: "nofile",
				Soft: 65535,
				Hard: 65535,
			})
		}
		// Replace the default template so the entrypoint generates our config.
		req.Files = append(req.Files, testcontainers.ContainerFile{
			Reader:            strings.NewReader(tunedAerospikeConf),
			ContainerFilePath: "/etc/aerospike/aerospike.template.conf",
			FileMode:          0o644,
		})
		return nil
	}
}

// fixtureConfig collects the harness-side knobs.
type fixtureConfig struct {
	aerospikeURL       string
	useBatch           bool
	batchMaxSize       int
	batchMaxWait       time.Duration
	batchMaxConcurrent int
	connQueueSize      int
	parentPoolSize     int
}

// newFixture spins up an Aerospike testcontainer (or connects to
// cfg.aerospikeURL if non-empty), constructs the validator + propagation
// stack, and pre-seeds the requested number of parent UTXOs.
func newFixture(ctx context.Context, cfg fixtureConfig) *fixture {
	tracing.SetupMockTracer()

	dt := newDummyT()
	logger := ulogger.New("loadtest")

	tSettings := test.CreateBaseTestSettings(dt)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Validator.UseBatchValidation = cfg.useBatch
	tSettings.Validator.BatchMaxSize = cfg.batchMaxSize
	tSettings.Validator.BatchMaxWait = cfg.batchMaxWait
	tSettings.Validator.BatchMaxConcurrent = cfg.batchMaxConcurrent
	// validator.New requires Kafka.TxMetaConfig to be non-nil even when
	// no Kafka producer is configured. The standalone binary doesn't
	// load settings.conf, so seed a dummy URL — it's only checked for
	// non-nilness; no Kafka client is started when the producer is nil.
	if tSettings.Kafka.TxMetaConfig == nil {
		dummyURL, _ := url.Parse("kafka://localhost:0/txmeta-disabled")
		tSettings.Kafka.TxMetaConfig = dummyURL
	}

	var (
		aeroURL       string
		containerFn   = func() {}
		containerName string
	)
	if cfg.aerospikeURL != "" {
		aeroURL = cfg.aerospikeURL
	} else {
		container, err := aeroTest.RunContainer(ctx,
			withTunedAerospikeConfig(),
			aeroTest.WithTTLSupport("test"),
		)
		if err != nil {
			log.Fatalf("fixture: aerospike container: %v", err)
		}
		containerName = container.GetContainerID()
		host, hostErr := container.Host(ctx)
		if hostErr != nil {
			log.Fatalf("fixture: container host: %v", hostErr)
		}
		port, portErr := container.ServicePort(ctx)
		if portErr != nil {
			log.Fatalf("fixture: container port: %v", portErr)
		}
		aeroURL = fmt.Sprintf(
			"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/loadtest-aero-external&ConnectionQueueSize=%d",
			host, port, cfg.connQueueSize,
		)
		containerFn = func() { _ = container.Terminate(ctx) }
	}

	u, err := url.Parse(aeroURL)
	if err != nil {
		log.Fatalf("fixture: parse url: %v", err)
	}

	aeroStore, err := aerostore.New(ctx, logger, tSettings, u)
	if err != nil {
		log.Fatalf("fixture: aerospike store: %v", err)
	}
	aeroStore.SetExternalStore(memory.New())

	vIface, err := validator.New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil, nil)
	if err != nil {
		log.Fatalf("fixture: validator: %v", err)
	}

	// Type-assert to *validator.Validator for PhaseSnapshot access.
	// If the assertion fails (shouldn't happen with current implementation),
	// we leave it nil and the telemetry sampler will skip PhaseSnapshot.
	var v *validator.Validator
	if concreteV, ok := vIface.(*validator.Validator); ok {
		v = concreteV
	}

	txStore := memory.New()
	ps := propagation.New(logger, tSettings, txStore, vIface, nil, nil, nil)

	if cfg.useBatch {
		ps.SetCoalescerForBench(propagation.NewTxCoalescer(
			ctx, logger, vIface,
			tSettings.Validator.BatchMaxSize,
			tSettings.Validator.BatchMaxWait,
			tSettings.Validator.BatchMaxConcurrent,
			tSettings.Validator.BatchCoalescerDrainMode,
		))
	}

	parents := seedParents(ctx, aeroStore, cfg.parentPoolSize)

	return &fixture{
		ps:            ps,
		aeroStore:     aeroStore,
		parents:       parents,
		containerName: containerName,
		validator:     v,
		cleanup: func() {
			ps.CloseCoalescerForBench(context.Background())
			containerFn()
			dt.cleanup()
		},
	}
}

// seedParents creates n parent records directly via aeroStore.Create
// (bypassing the validator). Each parent has one OP_TRUE output with
// 2000 satoshis so children can spend it with empty unlocking scripts.
// Parents are seeded in parallel using an errgroup bounded to NumCPU
// workers for fast pre-seeding of large pools.
func seedParents(ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	if n == 0 {
		return nil
	}
	opTrue, err := bscript.NewFromHexString("51") // OP_1 / OP_TRUE (anyone-can-spend)
	if err != nil {
		log.Fatalf("seedParents: opTrue: %v", err)
	}
	empty := &bscript.Script{}
	parents := make([]*bt.Tx, n)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(runtime.NumCPU())
	for i := 0; i < n; i++ {
		i := i
		g.Go(func() error {
			tx := bt.NewTx()
			// Unique random 32-byte PreviousTxID so every parent has a distinct tx
			// hash. PreviousTxOutIndex=0 (not 0xFFFFFFFF) ensures IsCoinbase()==false,
			// which prevents the Aerospike Lua script from applying the coinbase
			// maturity lock.
			var rb [32]byte
			if _, randErr := crand.Read(rb[:]); randErr != nil {
				return randErr
			}
			h, hashErr := chainhash.NewHash(rb[:])
			if hashErr != nil {
				return hashErr
			}
			in := &bt.Input{
				PreviousTxOutIndex: 0,
				PreviousTxScript:   empty,
				PreviousTxSatoshis: 2000, // input > output so fee = 1000 (valid)
				UnlockingScript:    empty,
				SequenceNumber:     0xFFFFFFFF,
			}
			if err := in.PreviousTxIDAdd(h); err != nil {
				return err
			}
			tx.Inputs = append(tx.Inputs, in)
			tx.Outputs = append(tx.Outputs, &bt.Output{
				Satoshis:      1000,
				LockingScript: opTrue,
			})
			if _, err := s.Create(gCtx, tx, 0); err != nil {
				return err
			}
			parents[i] = tx
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		log.Fatalf("seedParents: %v", err)
	}
	return parents
}

// buildChildSpending returns a single non-extended child tx that
// spends parent's output 0. Non-extended = realistic wire shape;
// the validator must hydrate PreviousTxSatoshis from the store at
// Phase A.
func buildChildSpending(parent *bt.Tx) *bt.Tx {
	child := bt.NewTx()
	ph := parent.TxIDChainHash()

	emptyScript := bscript.Script{}
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		// Deliberately leave PreviousTxSatoshis and PreviousTxScript unset
		// to simulate a non-extended (wire-format) transaction.
		UnlockingScript: &emptyScript,
		SequenceNumber:  0xFFFFFFFF,
	}
	if err := in.PreviousTxIDAdd(ph); err != nil {
		log.Fatalf("buildChildSpending: txid add: %v", err)
	}
	child.Inputs = append(child.Inputs, in)

	// Pay less than parent's 1000 satoshis so fee = 500 satoshis (valid once hydrated).
	if err := child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500); err != nil {
		log.Fatalf("buildChildSpending: payto: %v", err)
	}
	return child
}
