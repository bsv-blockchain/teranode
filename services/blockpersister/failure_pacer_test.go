package blockpersister

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFailurePacerEscalatesAndResets(t *testing.T) {
	base := 10 * time.Second
	p := &failurePacer{base: base, max: pacerMaxSleep}

	// First failure keeps the shipped interval: a store that hiccups is still
	// retried at the interval the operator configured.
	require.Equal(t, base, p.next())
	require.Equal(t, 2*base, p.next())
	require.Equal(t, 4*base, p.next())

	p.reset()
	require.Zero(t, p.consecutive())
	require.Equal(t, base, p.next(), "after a success the next failure starts at the shipped interval again")
}

func TestFailurePacerIsBounded(t *testing.T) {
	p := &failurePacer{base: time.Minute, max: pacerMaxSleep}

	var d time.Duration
	for i := 0; i < 12; i++ {
		d = p.next()
	}
	require.Equal(t, pacerMaxSleep, d, "escalation stops at the cap, it does not grow forever")
}

func TestFailurePacerAnnouncesOnce(t *testing.T) {
	p := &failurePacer{base: time.Second, max: pacerMaxSleep}

	var announced []int
	for i := 1; i <= 6; i++ {
		p.next()
		if p.announce() {
			announced = append(announced, p.consecutive())
		}
	}
	require.Equal(t, []int{announceAtFailures}, announced, "the operator warning fires exactly once per unbroken failure run")

	p.next()
	p.reset()
	p.next()
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
