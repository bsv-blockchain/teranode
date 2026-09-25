package utxoset

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The two guard counters of the containment build. Both are named exactly as the design names
// them, with no namespace prefix, so a dashboard or an abort criterion written from the design
// finds them by the same string.
//
// They are registered at package load rather than behind a sync.Once like the other stores'
// metrics, because nothing here is optional: a Store that exists writes them, and a test that
// reads one must find it registered whether or not a Store was ever opened.
var (
	// noIdentityReached counts an un-mine or a mark-off that reached a non-coinbase
	// transaction which has containment and no identity row. Such a transaction was created
	// through the block path at or below the checkpoint, was seeded, or has been stamped, and
	// no chain change the node admits should ever un-mine or mark off any of those. The
	// un-mine has by then deleted that transaction's only payload, so the counter must stay at
	// zero and is an abort criterion of the soak. Labelled by the operation that reached it.
	noIdentityReached = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_no_identity_reached_total",
		Help: "Un-mines and mark-offs that reached a non-coinbase transaction with containment and no identity row",
	}, []string{"op"})

	// The stamp's counters. Their healthy values at or below the checkpoint are stated where
	// they matter: below it tx_ident is empty, so pages stamp nothing and every no-winner and
	// suspect count is expected to be zero. A non-zero value there is an abort criterion of the
	// soak.
	stampWindows = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_windows_total",
		Help: "Containment windows whose stamp completed",
	})
	stampPages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_pages_total",
		Help: "Stamp pages committed",
	})
	stampUTXOs = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_utxos_total",
		Help: "Live UTXOs the stamp wrote a block onto",
	})
	stampIdentityDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_identity_deleted_total",
		Help: "Identity rows the stamp deleted, one per transaction stamped",
	})
	stampLosersDeleted = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_losers_deleted_total",
		Help: "Containment rows of blocks that lost a fork, deleted by the stamp's first step",
	})
	// stampNoWinner splits a transaction with no winner in its window by marker. Set is normal:
	// the transaction is unmined again. Null alarms: the transaction believes itself mined and
	// no chain-confirmed row exists for it anywhere, so the stamp set its marker.
	stampNoWinner = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_stamp_no_winner_total",
		Help: "Transactions with no winning containment row, by the state of their unmined marker",
	}, []string{"marker"})
	stampWinnerMarkerSet = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_winner_marker_set_total",
		Help: "Stamped transactions whose unmined marker was still set, the harmless direction of a wrong marker",
	})
	stampSuspectsDeferred = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_suspects_deferred_total",
		Help: "Suspects not judged because a block between the window and the anchor had mined_set false",
	})
	stampFenceLockTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_fence_lock_timeouts_total",
		Help: "Attempts at the stamp's first step that gave up waiting for the exclusive fence lock",
	})
	stampMissingWindows = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_missing_window_total",
		Help: "Windows with no table that the stamp advanced past as empty",
	})
	stampNotMinedAborts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_not_mined_aborts_total",
		Help: "Windows not stamped this drain because a main-chain block in them had mined_set false",
	})
	stampTwoWinners = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_two_winners_total",
		Help: "Pages rolled back because a transaction had two winning containment rows in one window",
	})
	stampAuditViolations = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_audit_violations_total",
		Help: "Sampled winners found with an identity row left or a live UTXO at (0,0) after the stamp; the window does not complete",
	})
	stampAuditDisagreements = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_audit_disagreements_total",
		Help: "Sampled live UTXOs whose pair is not the winner's",
	})
	stampCompletionMissing = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_stamp_completion_missing_total",
		Help: "Windows below the stamp-complete floor with no completion record, or a completion floor that moved under a drain; must stay at zero",
	})
	stampDrainsSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_stamp_drains_skipped_total",
		Help: "Drains not run, by reason",
	}, []string{"reason"})

	// The drop's counters.
	dropHeldByUndo = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_window_drop_held_by_undo_total",
		Help: "Containment windows due to drop but held because an attached undo partition covers a height below their stamped_at",
	})
	dropRefused = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_window_drop_refused_total",
		Help: "Containment window drops refused by the pre-drop check: an identity row is still joined to a row of the window",
	})
	dropDetachWaits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_window_detach_waits_total",
		Help: "Containment window detaches that did not finish within the timeout and were retried next block",
	})
	dropDetachRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "utxoset_window_detach_recovered_total",
		Help: "Containment windows found pending detach or already detached by an interrupted drop, and finished",
	})
	pruneStepErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "utxoset_prune_step_errors_total",
		Help: "Errors from one step of the prune pass, which does not stop the other steps",
	}, []string{"step"})
	retainIndefinitelyGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "utxoset_retain_indefinitely",
		Help: "1 while the operator has asked for every containment window and undo partition to be kept; the stamp still runs",
	})
)
