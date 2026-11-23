package banlist

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// loadBansQuery is the statement both loaders issue; saveBanFragment is
	// unique to Add's upsert. Gates match these, so text drift fails loudly.
	loadBansQuery   = "SELECT key, expiration_time, subnet FROM bans"
	saveBanFragment = "ON CONFLICT (key) DO UPDATE"
	contenderWindow = 100 * time.Millisecond
)

var activeUntil = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

// operationHarness runs operations in goroutines. Its cleanup, which runs
// before the fixture's pool close, releases every gate and joins them all.
type operationHarness struct {
	t  *testing.T
	fx *faultFixture
	wg sync.WaitGroup
}

func newOperationHarness(t *testing.T) *operationHarness {
	t.Helper()

	h := &operationHarness{t: t, fx: newFaultBanList(t)}
	t.Cleanup(func() {
		h.fx.instrument.releaseGates()
		h.wg.Wait()
	})

	return h
}

// operationCall signals when its goroutine actually begins the call and
// carries the call's result.
type operationCall struct {
	started chan struct{}
	result  chan error
}

func (h *operationHarness) start(operation func() error) *operationCall {
	call := &operationCall{started: make(chan struct{}), result: make(chan error, 1)}

	h.wg.Add(1)

	go func() {
		defer h.wg.Done()

		close(call.started)
		call.result <- operation()
	}()

	return call
}

// requireReadsDuringPause checks that ListBanned and IsBanned return within a
// bound while SQL is paused, and report the expected view.
func (h *operationHarness) requireReadsDuringPause(wantKeys []string, probe string, wantBanned bool) {
	h.t.Helper()

	type view struct {
		keys   []string
		banned bool
	}

	views := make(chan view, 1)

	h.wg.Add(1)

	go func() {
		defer h.wg.Done()

		views <- view{keys: h.fx.bl.ListBanned(), banned: h.fx.bl.IsBanned(probe)}
	}()

	select {
	case got := <-views:
		require.ElementsMatch(h.t, wantKeys, got.keys, "ListBanned while SQL paused")
		require.Equal(h.t, wantBanned, got.banned, "IsBanned(%q) while SQL paused", probe)
	case <-time.After(sqlGateTimeout):
		h.t.Fatal("ListBanned/IsBanned blocked while SQL was paused")
	}
}

func awaitOperation(t *testing.T, call *operationCall, name string) error {
	t.Helper()

	select {
	case err := <-call.result:
		return err
	case <-time.After(sqlGateTimeout):
		t.Fatalf("%s did not finish", name)

		return nil
	}
}

// requirePending is a bounded negative check supplementing the gate: the gate
// proves the first operation is paused in SQL, the start signal proves the
// contender's call began, and the window shows it did not complete meanwhile.
// The window cannot prove the contender would wait forever.
func requirePending(t *testing.T, call *operationCall, name string) {
	t.Helper()

	select {
	case <-call.started:
	case <-time.After(sqlGateTimeout):
		t.Fatalf("%s call never started", name)
	}

	select {
	case err := <-call.result:
		t.Fatalf("%s finished (err=%v) while an earlier local operation was paused in SQL", name, err)
	case <-time.After(contenderWindow):
	}
}

// TestBanList_OperationOrdering catches local Add, Remove, Clear and reload
// sequences interleaving across their SQL and map phases. Each scenario pauses
// the first operation at a real SQL point, checks reads still return (old
// snapshot during Remove/reload, memory-first view during Add), starts a
// contender that must wait, then releases and checks the serialized result:
// a removal cannot be undone by an earlier Add's save or an older reload
// snapshot, and a later Add or reload reflects the completed removal.
func TestBanList_OperationOrdering(t *testing.T) {
	initial := []string{"192.0.2.7", "[::ffff:192.0.2.7]:8333", "192.0.2.8"}

	removeHost := func(bl *BanList) error { return bl.Remove(context.Background(), "192.0.2.7") }
	addAlias := func(bl *BanList) error { return bl.Add(context.Background(), "192.0.2.7:9000", activeUntil) }
	addUnrelated := func(bl *BanList) error { return bl.Add(context.Background(), "198.51.100.9", activeUntil) }
	reload := func(bl *BanList) error { return bl.reloadFromDatabase() }
	clearAll := func(bl *BanList) error {
		bl.Clear()
		return nil
	}

	scenarios := []struct {
		name          string
		point         sqlGatePoint
		fragment      string
		first         func(*BanList) error
		contender     func(*BanList) error
		pausedKeys    []string
		pausedProbe   string
		pausedBanned  bool
		wantFinalKeys []string
	}{
		{
			name: "add_then_remove", point: gateBeforeStatement, fragment: saveBanFragment,
			first: addAlias, contender: removeHost,
			pausedKeys: append([]string{"192.0.2.7:9000"}, initial...), pausedProbe: "192.0.2.7", pausedBanned: true,
			wantFinalKeys: []string{"192.0.2.8"},
		},
		{
			name: "remove_then_add", point: gateBeforeStatement, fragment: deleteBanKey,
			first: removeHost, contender: addAlias,
			pausedKeys: initial, pausedProbe: "192.0.2.7", pausedBanned: true,
			wantFinalKeys: []string{"192.0.2.8", "192.0.2.7:9000"},
		},
		{
			name: "reload_then_remove", point: gateAfterRowsClose, fragment: loadBansQuery,
			first: reload, contender: removeHost,
			pausedKeys: initial, pausedProbe: "192.0.2.7", pausedBanned: true,
			wantFinalKeys: []string{"192.0.2.8"},
		},
		{
			name: "reload_then_add", point: gateAfterRowsClose, fragment: loadBansQuery,
			first: reload, contender: addUnrelated,
			pausedKeys: initial, pausedProbe: "198.51.100.9", pausedBanned: false,
			wantFinalKeys: append([]string{"198.51.100.9"}, initial...),
		},
		{
			name: "remove_then_reload", point: gateBeforeStatement, fragment: deleteBanKey,
			first: removeHost, contender: reload,
			pausedKeys: initial, pausedProbe: "192.0.2.7", pausedBanned: true,
			wantFinalKeys: []string{"192.0.2.8"},
		},
		{
			name: "remove_then_clear", point: gateBeforeStatement, fragment: deleteBanKey,
			first: removeHost, contender: clearAll,
			pausedKeys: initial, pausedProbe: "192.0.2.7", pausedBanned: true,
			wantFinalKeys: []string{},
		},
	}

	for _, tc := range scenarios {
		t.Run(tc.name, func(t *testing.T) {
			h := newOperationHarness(t)
			bl := h.fx.bl
			loadSeededBans(t, bl,
				activeBan("192.0.2.7", "192.0.2.7/32"),
				activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"),
				activeBan("192.0.2.8", "192.0.2.8/32"),
			)

			gate := h.fx.instrument.armGate(tc.point, tc.fragment)
			first := h.start(func() error { return tc.first(bl) })
			gate.awaitEntered(t)

			h.requireReadsDuringPause(tc.pausedKeys, tc.pausedProbe, tc.pausedBanned)

			contender := h.start(func() error { return tc.contender(bl) })
			requirePending(t, contender, "contender")

			gate.open()
			require.NoError(t, awaitOperation(t, first, "first operation"))
			require.NoError(t, awaitOperation(t, contender, "contender"))
			requireBanState(t, bl, tc.wantFinalKeys, tc.wantFinalKeys)
		})
	}
}

func runLoader(bl *BanList, mode string) error {
	if mode == "load" {
		return bl.LoadFromDatabase(context.Background())
	}

	return bl.reloadFromDatabase()
}

// seedMemoryOnlyBan leaves key in memory with no SQL row.
func seedMemoryOnlyBan(t *testing.T, bl *BanList, key, subnet string) {
	t.Helper()

	loadSeededBans(t, bl, activeBan(key, subnet))

	_, err := bl.db.ExecContext(context.Background(), "DELETE FROM bans WHERE key = $1", key)
	require.NoError(t, err)
}

// TestBanList_LoadPublication catches loaders publishing rows before reading
// completes: a row error after one decoded row must leave memory untouched,
// and reads during a paused load must see the previous snapshot. Load keeps
// merging and reload keeps replacing, with networks derived from raw keys.
func TestBanList_LoadPublication(t *testing.T) {
	t.Run("load_merges_reload_replaces", func(t *testing.T) {
		bl := newTestBanList(t)
		seedMemoryOnlyBan(t, bl, "198.51.100.7", "198.51.100.7/32")
		seedBans(t, bl, activeBan("::ffff:192.0.2.7", "::/32"), activeBan("192.0.2.0/24", "0.0.0.0/0"))

		require.NoError(t, bl.LoadFromDatabase(context.Background()))
		require.ElementsMatch(t, []string{"198.51.100.7", "::ffff:192.0.2.7", "192.0.2.0/24"}, bl.ListBanned())

		peers := bl.BannedPeers()
		require.Equal(t, "192.0.2.7/32", peers["::ffff:192.0.2.7"].Subnet.String())
		require.Equal(t, "192.0.2.0/24", peers["192.0.2.0/24"].Subnet.String())

		require.NoError(t, bl.reloadFromDatabase())
		require.ElementsMatch(t, []string{"::ffff:192.0.2.7", "192.0.2.0/24"}, bl.ListBanned())
	})

	for _, mode := range []string{"load", "reload"} {
		t.Run(mode+"/no_partial_publish_after_row_error", func(t *testing.T) {
			fx := newFaultBanList(t)
			bl := fx.bl
			seedMemoryOnlyBan(t, bl, "198.51.100.7", "198.51.100.7/32")
			seedBans(t, bl,
				activeBan("192.0.2.7", "192.0.2.7/32"),
				activeBan("192.0.2.8", "192.0.2.8/32"),
				activeBan("2001:db8::7", "2001:db8::7/128"),
			)

			before := bl.BannedPeers()
			rule := fx.instrument.arm(phaseRowsNextRow, loadBansQuery, 1, 1, injectedSQLFault)

			err := runLoader(bl, mode)
			fx.instrument.disarm()

			require.Error(t, err)
			require.Equal(t, 1, fx.instrument.firedCount(rule), "fault never reached the loader")
			require.Equal(t, before, bl.BannedPeers(), "a failed %s must not publish decoded rows", mode)
		})

		t.Run(mode+"/reads_during_paused_load", func(t *testing.T) {
			h := newOperationHarness(t)
			bl := h.fx.bl
			seedMemoryOnlyBan(t, bl, "198.51.100.7", "198.51.100.7/32")
			seedBans(t, bl, activeBan("192.0.2.7", "192.0.2.7/32"), activeBan("192.0.2.8", "192.0.2.8/32"))

			gate := h.fx.instrument.armGate(gateAfterRowsClose, loadBansQuery)
			result := h.start(func() error { return runLoader(bl, mode) })
			gate.awaitEntered(t)

			h.requireReadsDuringPause([]string{"198.51.100.7"}, "192.0.2.7", false)

			gate.open()
			require.NoError(t, awaitOperation(t, result, mode))

			want := []string{"192.0.2.7", "192.0.2.8"}
			if mode == "load" {
				want = append(want, "198.51.100.7")
			}

			require.ElementsMatch(t, want, bl.ListBanned())
		})
	}
}

// requireAddAck consumes the add notification for key, so later event
// assertions cannot be confused by a late add delivery.
func requireAddAck(t *testing.T, events chan BanEvent, key string) {
	t.Helper()

	select {
	case event := <-events:
		require.Equal(t, "add", event.Action)
		require.Equal(t, key, event.IP)
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for add event for %q", key)
	}
}

// TestBanList_OperationFailureCompatibility pins existing failure semantics
// that the ordering change must preserve. A failed Add save leaves the
// memory-only ban and its add event, and a later equivalent Remove clears that
// alias. A failed Clear leaves memory empty and SQL intact, emits no clear
// event, keeps subscriptions, and reload restores the persisted state.
func TestBanList_OperationFailureCompatibility(t *testing.T) {
	t.Run("failed_add_then_equivalent_remove", func(t *testing.T) {
		bl := newTestBanList(t)
		ctx := context.Background()
		const key = "[::ffff:192.0.2.7]:8333"

		execSQL(t, bl.db, `CREATE TRIGGER abort_ban_insert BEFORE INSERT ON bans
		BEGIN
			SELECT RAISE(ABORT, 'injected ban insert failure');
		END`)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		require.Error(t, bl.Add(ctx, key, activeUntil))
		requireHostEvent(t, events, "add", key)
		require.ElementsMatch(t, []string{key}, bl.ListBanned(), "failed save keeps the memory-first ban")
		require.Empty(t, persistedKeys(t, bl))
		require.True(t, bl.IsBanned("192.0.2.7"))

		execSQL(t, bl.db, "DROP TRIGGER abort_ban_insert")

		require.NoError(t, bl.Remove(ctx, "192.0.2.7"))
		require.Equal(t, hostEvents(key), collectRemoveEvents(t, events, 1))
		requireNoEvent(t, events)
		requireBanState(t, bl, []string{}, []string{})
		require.False(t, bl.IsBanned("192.0.2.7"))
	})

	t.Run("failed_clear_then_reload", func(t *testing.T) {
		bl := newTestBanList(t)
		persisted := []string{"192.0.2.7", "192.0.2.0/24"}
		loadSeededBans(t, bl, activeBan("192.0.2.7", "192.0.2.7/32"), activeBan("192.0.2.0/24", "192.0.2.0/24"))

		execSQL(t, bl.db, `CREATE TRIGGER abort_ban_delete BEFORE DELETE ON bans
		BEGIN
			SELECT RAISE(ABORT, 'injected ban delete failure');
		END`)

		events := bl.Subscribe()
		defer bl.Unsubscribe(events)

		bl.Clear()

		requireBanState(t, bl, []string{}, persisted)
		requireNoEvent(t, events)

		execSQL(t, bl.db, "DROP TRIGGER abort_ban_delete")
		require.NoError(t, bl.reloadFromDatabase())
		requireBanState(t, bl, persisted, persisted)

		// The subscription survived the failed Clear.
		require.NoError(t, bl.Add(context.Background(), "192.0.2.7:8333", activeUntil))
		requireHostEvent(t, events, "add", "192.0.2.7:8333")
		requireNoEvent(t, events)
	})
}

// TestBanList_RemoveExpiryOverlap catches removal depending on expired aliases
// staying in memory, and removal touching unrelated keys. Remove is paused in
// SQL after capturing its memory selection; an uncovered lookup then evicts
// the expired alias from memory only. Removal must still delete that alias
// from SQL and announce it, while an unrelated host renewed beforehand
// survives in memory and SQL.
func TestBanList_RemoveExpiryOverlap(t *testing.T) {
	h := newOperationHarness(t)
	bl := h.fx.bl
	ctx := context.Background()

	const expiredAlias, activeAlias, renewedHost = "192.0.2.7:8333", "[::ffff:192.0.2.7]:8334", "198.51.100.7"

	loadSeededBans(t, bl,
		expiredBan(expiredAlias, "192.0.2.7/32"),
		activeBan(activeAlias, "192.0.2.7/32"),
		expiredBan(renewedHost, "198.51.100.7/32"),
	)

	events := bl.Subscribe()
	defer bl.Unsubscribe(events)

	require.NoError(t, bl.Add(ctx, renewedHost, activeUntil))
	requireAddAck(t, events, renewedHost)

	gate := h.fx.instrument.armGate(gateBeforeStatement, deleteBanKey)
	remove := h.start(func() error { return bl.Remove(ctx, "192.0.2.7") })
	gate.awaitEntered(t)

	// ListBanned is read before the uncovered lookup, which traverses every entry
	// and evicts expired ones from memory only.
	h.requireReadsDuringPause([]string{expiredAlias, activeAlias, renewedHost}, "203.0.113.1", false)
	require.ElementsMatch(t, []string{activeAlias, renewedHost}, bl.ListBanned(), "expired alias evicted from memory")
	require.ElementsMatch(t, []string{expiredAlias, activeAlias, renewedHost}, persistedKeys(t, bl), "eviction leaves SQL intact")

	gate.open()
	require.NoError(t, awaitOperation(t, remove, "Remove"))

	require.Equal(t, hostEvents(expiredAlias, activeAlias), collectRemoveEvents(t, events, 2))
	requireNoEvent(t, events)
	requireBanState(t, bl, []string{renewedHost}, []string{renewedHost})
	require.True(t, bl.IsBanned(renewedHost))

	require.NoError(t, bl.reloadFromDatabase())
	require.ElementsMatch(t, []string{renewedHost}, bl.ListBanned())
}

// TestBanList_RemoveSharedDatabase covers two BanLists over one real store
// with no concurrent writer: the reader stays stale until it reloads, then
// sees every alias removed and the covering CIDR preserved. The reader emits
// no events for another instance's removal. Cross-instance linearizability is
// not asserted.
func TestBanList_RemoveSharedDatabase(t *testing.T) {
	remover := newTestBanList(t)
	ctx := context.Background()

	seedBans(t, remover,
		activeBan("192.0.2.7", "192.0.2.7/32"),
		activeBan("[::ffff:192.0.2.7]:8333", "192.0.2.7/32"),
		expiredBan("::ffff:c000:207", "192.0.2.7/32"),
		activeBan("192.0.2.0/24", "192.0.2.0/24"),
		activeBan("198.51.100.7", "198.51.100.7/32"),
	)
	require.NoError(t, remover.LoadFromDatabase(ctx))

	reader := New(remover.db, remover.engine, remover.logger)
	require.NoError(t, reader.Init(ctx))

	all := []string{"192.0.2.7", "[::ffff:192.0.2.7]:8333", "::ffff:c000:207", "192.0.2.0/24", "198.51.100.7"}
	remaining := []string{"192.0.2.0/24", "198.51.100.7"}
	require.ElementsMatch(t, all, reader.ListBanned())

	readerEvents := reader.Subscribe()
	defer reader.Unsubscribe(readerEvents)

	require.NoError(t, remover.Remove(ctx, "192.0.2.7:1"))
	requireBanState(t, remover, remaining, remaining)

	require.ElementsMatch(t, all, reader.ListBanned(), "reader keeps its snapshot until reload")
	requireNoEvent(t, readerEvents)

	require.NoError(t, reader.reloadFromDatabase())
	require.ElementsMatch(t, remaining, reader.ListBanned())
	require.True(t, reader.IsBanned("192.0.2.7"), "covering CIDR still enforces after reload")
	require.False(t, reader.IsBanned("192.0.2.8:1") && !reader.IsBanned("192.0.2.8"))
}
