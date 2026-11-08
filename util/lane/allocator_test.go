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
// position ever handed out in it, and a request with no lane gets the lowest
// lane nobody holds.
type fakeSource struct {
	mu      sync.Mutex
	size    uint64
	highest map[uint8]uint64
	holders map[uint8]string
	down    bool
	calls   []fakeCall
}

type fakeCall struct {
	holder   string
	wantLane uint8
	hasLane  bool
}

func newFakeSource(size uint64) *fakeSource {
	return &fakeSource{size: size, highest: map[uint8]uint64{}, holders: map[uint8]string{}}
}

func (f *fakeSource) ReserveLaneRange(_ context.Context, holder string, wantLane uint8, hasLane bool) (Range, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, fakeCall{holder, wantLane, hasLane})

	if f.down {
		return Range{}, errors.NewServiceUnavailableError("blockchain service down")
	}

	l := wantLane
	if !hasLane || (f.holders[l] != "" && f.holders[l] != holder) {
		l = 0
		for f.holders[l] != "" {
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
	src.holders[4] = "someone else"

	file := filepath.Join(t.TempDir(), "lane")
	require.NoError(t, os.WriteFile(file, []byte("4\n"), 0o600))

	a := startAllocator(t, src, "pod-3", file)

	require.Equal(t, fakeCall{"pod-3", 4, true}, src.firstCall())
	require.Equal(t, uint8(0), Lane(next(t, a)), "lane 4 is someone else's, so the service gave lane 0")

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

func TestFileNameNeverLeavesTheLaneFolder(t *testing.T) {
	require.Equal(t, "teranode-1.lane", FileName("teranode-1"))
	require.Equal(t, "___etc_passwd.lane", FileName("../etc/passwd"))
	require.Equal(t, "default.lane", FileName(""))
}
