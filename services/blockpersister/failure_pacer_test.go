package blockpersister

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

var (
	testBlockA = chainhash.HashH([]byte("pacer-block-a"))
	testBlockB = chainhash.HashH([]byte("pacer-block-b"))
)

func TestFailurePacerEscalatesAndResets(t *testing.T) {
	base := 10 * time.Second
	p := &failurePacer{base: base, max: pacerMaxSleep}

	// First failure keeps the shipped interval: a store that hiccups is still
	// retried at the interval the operator configured.
	require.Equal(t, base, p.next(&testBlockA))
	require.Equal(t, 2*base, p.next(&testBlockA))
	require.Equal(t, 4*base, p.next(&testBlockA))

	p.reset()
	require.Zero(t, p.consecutive())
	require.Equal(t, base, p.next(&testBlockA), "after a success the next failure starts at the shipped interval again")
}

func TestFailurePacerIsBounded(t *testing.T) {
	p := &failurePacer{base: time.Minute, max: pacerMaxSleep}

	var d time.Duration
	for i := 0; i < 12; i++ {
		d = p.next(&testBlockA)
	}
	require.Equal(t, pacerMaxSleep, d, "escalation stops at the cap, it does not grow forever")
}

func TestFailurePacerAnnouncesOnce(t *testing.T) {
	p := &failurePacer{base: time.Second, max: pacerMaxSleep}

	var announced []int
	for i := 1; i <= 6; i++ {
		p.next(&testBlockA)
		if p.announce() {
			announced = append(announced, p.consecutive())
		}
	}
	require.Equal(t, []int{announceAtFailures}, announced, "the operator warning fires exactly once per unbroken failure run")

	p.next(&testBlockA)
	p.reset()
	p.next(&testBlockA)
	require.False(t, p.announce(), "a fresh run below the threshold stays quiet")
}

func TestSleepForHonoursContext(t *testing.T) {
	s := &Server{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	s.sleepFor(ctx, time.Hour)
	require.Less(t, time.Since(start), time.Second, "a done context returns at once")
}

func TestFailurePacerRestartsOnADifferentBlock(t *testing.T) {
	base := 10 * time.Second
	p := &failurePacer{base: base, max: pacerMaxSleep}

	for i := 0; i < announceAtFailures; i++ {
		p.next(&testBlockA)
	}
	require.Equal(t, announceAtFailures, p.consecutive())

	require.Equal(t, base, p.next(&testBlockB), "a new block starts at the shipped interval, not the escalated one")
	require.Equal(t, 1, p.consecutive())
	require.False(t, p.announce())
}

// The processing loop calls pacedSleep with the block it just failed on. A block
// that fails after another block's escalation must wait the shipped interval and
// still earn its own operator announcement.
func TestPacedSleepIsPerBlock(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockPersister.PersistSleep = time.Millisecond

	s := &Server{logger: ulogger.NewVerboseTestLogger(t), settings: tSettings}
	pacer := &failurePacer{base: tSettings.BlockPersister.PersistSleep, max: pacerMaxSleep}
	ctx := context.Background()

	for i := 0; i < announceAtFailures+2; i++ {
		s.pacedSleep(ctx, pacer, &testBlockA)
	}
	require.Equal(t, announceAtFailures+2, pacer.consecutive())

	s.pacedSleep(ctx, pacer, &testBlockB)
	require.Equal(t, 1, pacer.consecutive(), "block B must not inherit block A's failure run")

	pacer.reset()
	require.Zero(t, pacer.consecutive())
}
