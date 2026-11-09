package lane

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// fakeSource stands in for the blockchain service at the RangeSource boundary.
// It follows the service's rules: a lane's next range starts above the highest
// position ever handed out in it, a named lane is granted unless it is
// exhausted, and a request with no lane gets the lowest lane nobody holds.
type fakeSource struct {
	mu      sync.Mutex
	size    uint64
	highest map[uint8]uint64
	holders map[uint8]string
	// exhausted marks lanes whose positions have all been handed out.
	exhausted map[uint8]bool
	down      bool
	calls     []fakeCall
}

type fakeCall struct {
	holder   string
	wantLane uint8
	hasLane  bool
}

func newFakeSource(size uint64) *fakeSource {
	return &fakeSource{size: size, highest: map[uint8]uint64{}, holders: map[uint8]string{}, exhausted: map[uint8]bool{}}
}

func (f *fakeSource) ReserveLaneRange(_ context.Context, holder string, wantLane uint8, hasLane bool) (Range, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{holder, wantLane, hasLane})

	if f.down {
		return Range{}, errors.NewServiceUnavailableError("blockchain service down")
	}

	l := wantLane
	if !hasLane || f.exhausted[l] {
		l = 0
		for f.holders[l] != "" || f.exhausted[l] {
			l++
		}
	}

	f.holders[l] = holder
	r := Range{Lane: l, First: f.highest[l] + 1, Last: f.highest[l] + f.size}
	f.highest[l] = r.Last

	return r, nil
}

func (f *fakeSource) setDown(down bool) {
	f.mu.Lock()
	f.down = down
	f.mu.Unlock()
}

func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

func (f *fakeSource) firstCall() fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls[0]
}

func startAllocator(t *testing.T, src RangeSource, holder, file string) *Allocator {
	t.Helper()

	a := NewAllocator(src, holder, file, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, a.Start(ctx))
	t.Cleanup(a.Close)

	return a
}

// next takes one lane position and fails the test if none is in hand.
func next(t *testing.T, a *Allocator) uint64 {
	t.Helper()

	v, ok := a.Next()
	require.True(t, ok, "the allocator has no range in hand")

	return v
}

func TestAllocatorHandsOutConsecutivePositionsInItsLane(t *testing.T) {
	src := newFakeSource(1000)
	a := startAllocator(t, src, "pod-3", filepath.Join(t.TempDir(), "lane"))

	require.Equal(t, Pack(0, 1), next(t, a))
	require.Equal(t, Pack(0, 2), next(t, a))
	require.Equal(t, Pack(0, 3), next(t, a))
}

func TestAllocatorWithNoSavedLaneAsksForAnyAndSavesWhatItGets(t *testing.T) {
	src := newFakeSource(1000)
	src.holders[0] = "someone else"

	file := filepath.Join(t.TempDir(), "lane")
	a := startAllocator(t, src, "pod-3", file)

	require.False(t, src.firstCall().hasLane, "no file means no lane to ask for")
	require.Equal(t, uint8(1), Lane(next(t, a)))

	saved, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "1\n", string(saved))
}

func TestAllocatorAsksForItsSavedLaneAfterARestart(t *testing.T) {
	src := newFakeSource(1000)
	src.holders[0] = "someone else"
	file := filepath.Join(t.TempDir(), "lane")

	first := startAllocator(t, src, "pod-3", file)

	var last uint64
	for i := 0; i < 1500; i++ { // past the end of the first range
		last = next(t, first)
	}

	first.Close()

	restarted := newFakeSourceSharing(src)
	second := startAllocator(t, restarted, "pod-3", file)

	require.Equal(t, fakeCall{"pod-3", 1, true}, restarted.firstOwnCall(), "the restart asks for the saved lane")

	v := next(t, second)
	require.Equal(t, Lane(last), Lane(v), "a restart carries on in the same lane")

	_, lastPos := Unpack(last)
	_, pos := Unpack(v)
	require.Greater(t, pos, lastPos, "positions only increase within a lane across a restart")
}

// sharedSource is the same service seen from a new process: it shares the
// service's state but records its own calls.
type sharedSource struct {
	*fakeSource
	ownCalls []fakeCall
}

func newFakeSourceSharing(f *fakeSource) *sharedSource {
	return &sharedSource{fakeSource: f}
}

func (s *sharedSource) ReserveLaneRange(ctx context.Context, holder string, wantLane uint8, hasLane bool) (Range, error) {
	s.fakeSource.mu.Lock()
	s.ownCalls = append(s.ownCalls, fakeCall{holder, wantLane, hasLane})
	s.fakeSource.mu.Unlock()

	return s.fakeSource.ReserveLaneRange(ctx, holder, wantLane, hasLane)
}

func (s *sharedSource) firstOwnCall() fakeCall {
	s.fakeSource.mu.Lock()
	defer s.fakeSource.mu.Unlock()

	return s.ownCalls[0]
}

func TestAllocatorMovesToTheLaneTheServiceGivesAndSavesIt(t *testing.T) {
	src := newFakeSource(1000)
	src.exhausted[4] = true

	file := filepath.Join(t.TempDir(), "lane")
	require.NoError(t, os.WriteFile(file, []byte("4\n"), 0o600))

	a := startAllocator(t, src, "pod-3", file)

	require.Equal(t, fakeCall{"pod-3", 4, true}, src.firstCall())
	require.Equal(t, uint8(0), Lane(next(t, a)), "lane 4 is exhausted, so the service gave lane 0")

	saved, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "0\n", string(saved))
}

func TestAllocatorHoldsASpareRangeSoTheNextOneIsAlreadyInHand(t *testing.T) {
	src := newFakeSource(10)
	a := startAllocator(t, src, "pod-3", filepath.Join(t.TempDir(), "lane"))

	require.Eventually(t, func() bool { return src.callCount() == 2 }, 2*time.Second, time.Millisecond,
		"the allocator fetches the next range before the first runs out")

	// The service goes away. Both ranges in hand, 20 positions, still hand out.
	src.setDown(true)

	for i := uint64(1); i <= 20; i++ {
		require.Equal(t, Pack(0, i), next(t, a))
	}

	_, ok := a.Next()
	require.False(t, ok, "with both ranges used and the service down there is nothing to hand out")

	src.setDown(false)

	require.Eventually(t, func() bool { _, ok := a.Next(); return ok }, 5*time.Second, 5*time.Millisecond,
		"once the service is back the allocator refills")
}

func TestAllocatorNeverHandsOutAPositionTwice(t *testing.T) {
	src := newFakeSource(100)
	a := startAllocator(t, src, "pod-3", filepath.Join(t.TempDir(), "lane"))

	const goroutines, each = 16, 500

	got := make(chan uint64, goroutines*each)

	var wg sync.WaitGroup

	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < each; i++ {
				for {
					if v, ok := a.Next(); ok {
						got <- v
						break
					}

					time.Sleep(time.Microsecond)
				}
			}
		}()
	}

	wg.Wait()
	close(got)

	seen := map[uint64]bool{}
	for v := range got {
		require.False(t, seen[v], "position %x handed out twice", v)
		seen[v] = true
	}

	require.Len(t, seen, goroutines*each)
}

func TestAllocatorFillFillsAsManyAsItHas(t *testing.T) {
	src := newFakeSource(4)
	a := startAllocator(t, src, "pod-3", filepath.Join(t.TempDir(), "lane"))

	require.Eventually(t, func() bool { return src.callCount() == 2 }, 2*time.Second, time.Millisecond)
	src.setDown(true)

	dst := make([]uint64, 6)
	require.Equal(t, 6, a.Fill(dst))
	require.Equal(t, []uint64{Pack(0, 1), Pack(0, 2), Pack(0, 3), Pack(0, 4), Pack(0, 5), Pack(0, 6)}, dst)

	dst = make([]uint64, 6)
	require.Equal(t, 2, a.Fill(dst), "two positions were left")
	require.Equal(t, []uint64{Pack(0, 7), Pack(0, 8), None, None, None, None}, dst)
}

func TestAllocatorStartWithTheServiceDownStillRuns(t *testing.T) {
	src := newFakeSource(10)
	src.setDown(true)

	a := NewAllocator(src, "pod-3", filepath.Join(t.TempDir(), "lane"), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.Error(t, a.Start(ctx), "Start reports that no range came")
	t.Cleanup(a.Close)

	_, ok := a.Next()
	require.False(t, ok)

	src.setDown(false)

	require.Eventually(t, func() bool { _, ok := a.Next(); return ok }, 5*time.Second, 5*time.Millisecond,
		"the allocator keeps trying in the background")
}

// With the current range spent and a spare held, the allocator must not fetch: a newer range
// would go in ahead of the spare, and positions in the lane would go backwards.
func TestAllocatorPositionsNeverGoBackwards(t *testing.T) {
	const rangeSize = 10

	r1 := Range{Lane: 2, First: 1, Last: rangeSize}
	r2 := Range{Lane: 2, First: rangeSize + 1, Last: 2 * rangeSize}
	r3 := Range{Lane: 2, First: 2*rangeSize + 1, Last: 3 * rangeSize}

	// Not started: the test delivers the ranges itself, one state at a time.
	a := NewAllocator(newFakeSource(rangeSize), "pod-3", filepath.Join(t.TempDir(), "lane"), nil)
	t.Cleanup(a.Close)

	a.install(r1)
	a.install(r2)

	var last uint64
	for i := uint64(0); i < r1.Len(); i++ {
		last = next(t, a)
	}

	need, _, _ := a.needsRange()
	require.False(t, need, "the current range is spent, but with a spare held there is nothing to fetch")

	// A range delivered in that state still must not go in ahead of what is held.
	a.install(r3)

	for {
		v, ok := a.Next()
		if !ok {
			break
		}

		require.Greater(t, v, last, "positions in a lane only increase")
		last = v
	}
}

// The same rule under load: small ranges, so the swap and the fetch happen thousands of times
// while creates race them. Any one goroutine must see its own positions rise.
func TestAllocatorPositionsRiseUnderConcurrentCreates(t *testing.T) {
	src := newFakeSource(3)
	a := startAllocator(t, src, "pod-3", filepath.Join(t.TempDir(), "lane"))

	const goroutines, each = 8, 2000

	var wg sync.WaitGroup

	failed := make(chan string, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			var last uint64

			for i := 0; i < each; i++ {
				v, ok := a.Next()
				for !ok {
					time.Sleep(time.Microsecond)

					v, ok = a.Next()
				}

				if v <= last {
					failed <- "a goroutine saw its position go backwards"
					return
				}

				last = v
			}
		}()
	}

	wg.Wait()
	close(failed)

	for msg := range failed {
		require.Fail(t, msg)
	}
}

// scriptedSource answers from a script in order, then holds its last answer behind a gate.
type scriptedSource struct {
	mu     sync.Mutex
	script []Range
	calls  int
	gate   chan struct{}
	final  Range
}

func (s *scriptedSource) ReserveLaneRange(ctx context.Context, _ string, _ uint8, _ bool) (Range, error) {
	s.mu.Lock()
	s.calls++
	calls := s.calls
	s.mu.Unlock()

	if calls <= len(s.script) {
		return s.script[calls-1], nil
	}

	select {
	case <-s.gate:
		return s.final, nil
	case <-ctx.Done():
		return Range{}, ctx.Err()
	}
}

func (s *scriptedSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// The service is trusted to hand out good ranges, but a bad one must not panic in Pack on the
// create path, or spin the fetch loop with no backoff.
func TestAllocatorRefusesAnInvalidRangeAndKeepsTrying(t *testing.T) {
	good := Range{Lane: 4, First: 7, Last: 9}

	src := &scriptedSource{
		script: []Range{
			{Lane: 4},                    // the zero range
			{Lane: 4, First: 0, Last: 5}, // position 0 means "no position"
			{Lane: 4, First: 9, Last: 3}, // empty, last below first
			{Lane: 4, First: MaxPosition, Last: MaxPosition + 5}, // does not fit in seven bytes
		},
		gate:  make(chan struct{}),
		final: good,
	}

	a := NewAllocator(src, "pod-3", filepath.Join(t.TempDir(), "lane"), nil)
	t.Cleanup(a.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	require.Error(t, a.Start(ctx), "no valid range has come yet")

	require.Eventually(t, func() bool { return src.callCount() > len(src.script) }, 10*time.Second, time.Millisecond,
		"each invalid range is retried")

	_, ok := a.Next()
	require.False(t, ok, "no invalid range was installed")

	close(src.gate)

	require.Eventually(t, func() bool {
		v, ok := a.Next()
		if ok {
			require.Equal(t, Pack(good.Lane, good.First), v)
		}

		return ok
	}, 10*time.Second, time.Millisecond, "the valid range is used once it comes")
}

// hangingSource hangs its first call until the caller's context ends, then answers normally.
type hangingSource struct {
	*fakeSource
	mu       sync.Mutex
	hung     bool
	deadline bool
}

func (s *hangingSource) ReserveLaneRange(ctx context.Context, holder string, wantLane uint8, hasLane bool) (Range, error) {
	s.mu.Lock()
	first := !s.hung
	s.hung = true

	if first {
		_, s.deadline = ctx.Deadline()
	}
	s.mu.Unlock()

	if first {
		<-ctx.Done()

		return Range{}, ctx.Err()
	}

	return s.fakeSource.ReserveLaneRange(ctx, holder, wantLane, hasLane)
}

// A hand-out that hangs must become an error and take the log and backoff path. Only Close ends
// the fetch loop's own context, so with no per-attempt bound this test would never get a range.
func TestAllocatorGivesUpOnAHungRequestAndRetries(t *testing.T) {
	src := &hangingSource{fakeSource: newFakeSource(10)}

	a := NewAllocator(src, "pod-3", filepath.Join(t.TempDir(), "lane"), nil)
	a.attemptTimeout = 50 * time.Millisecond

	t.Cleanup(a.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, a.Start(ctx), "the retry after the hung call brings a range")

	src.mu.Lock()
	defer src.mu.Unlock()

	require.True(t, src.deadline, "each attempt carries its own deadline")
}

func TestFileNameNeverLeavesTheLaneFolder(t *testing.T) {
	require.Equal(t, "teranode-1.lane", FileName("teranode-1"))
	require.Equal(t, "___etc_passwd.lane", FileName("../etc/passwd"))
	require.Equal(t, "default.lane", FileName(""))
}
