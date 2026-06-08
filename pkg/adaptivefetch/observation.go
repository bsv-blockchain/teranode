package adaptivefetch

// Observation is a single measurement fed to State.Record.
//
// TotalTxs, LocalHits and MissingFetches are counts evaluated against
// State.mode at the time of Record. Callers must record only observations
// whose underlying work was performed in the current runtime mode — the
// state machine has no way to disambiguate cross-mode samples in the window.
type Observation struct {
	TotalTxs       int
	LocalHits      int
	MissingFetches int
}
