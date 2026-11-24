package parity

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/stretchr/testify/require"
)

// Observation is what a scenario could see of a node from the outside: the
// scripted peers' transcripts and the node's active chain. Field names are the
// vocabulary Compare and the reports use.
type Observation struct {
	// BlocksAccepted is the node's active chain height after Drive.
	BlocksAccepted uint32
	// Requests is, per peer address, how many block getdata items that peer received.
	Requests map[string]int
	// Served is, per peer address, how many block messages that peer sent.
	Served map[string]int
	// Disconnected is, per peer address, who closed the connection: "node" or "peer".
	Disconnected map[string]string
	// Scores is, per peer address, the node's misbehaviour total for that peer.
	Scores map[string]int
	// Connections is, per peer address, how many times the node connected.
	// Reported, never compared: redial cadence is transport policy.
	Connections map[string]int
	// GetHeadersIn is the total getheaders every peer received from the node.
	// Reported and available to scenarios; not compared.
	GetHeadersIn int
	// Notes are free-text facts a scenario wants in the report (for instance the
	// disconnect reason each side logged). Reported, never compared.
	Notes map[string]string
	// WallClock is how long Drive took. Reported, never compared.
	WallClock time.Duration
}

// Divergence names an Observation field whose difference between the two
// implementations is expected, and why. Field is the bare field name
// ("Requests"), which covers every key of a map field.
type Divergence struct {
	Field  string
	Reason string
}

// Result is what Compare found: Diffs fail the scenario, Accepted are logged.
type Result struct {
	Diffs    []string
	Accepted []string
}

// Compare diffs two observations field by field. A field named in accepted
// moves its differences into Result.Accepted with the reason attached.
func Compare(legacy, svp2p Observation, accepted []Divergence) Result {
	reasons := make(map[string]string, len(accepted))
	for _, d := range accepted {
		reasons[d.Field] = d.Reason
	}

	var res Result

	emit := func(field, line string) {
		if reason, ok := reasons[field]; ok {
			res.Accepted = append(res.Accepted, fmt.Sprintf("%s (accepted: %s)", line, reason))
			return
		}

		res.Diffs = append(res.Diffs, line)
	}

	if legacy.BlocksAccepted != svp2p.BlocksAccepted {
		emit("BlocksAccepted", fmt.Sprintf("BlocksAccepted: legacy=%d svp2p=%d", legacy.BlocksAccepted, svp2p.BlocksAccepted))
	}

	compareIntMap(emit, "Requests", legacy.Requests, svp2p.Requests)
	compareIntMap(emit, "Served", legacy.Served, svp2p.Served)
	compareStringMap(emit, "Disconnected", legacy.Disconnected, svp2p.Disconnected)
	compareIntMap(emit, "Scores", legacy.Scores, svp2p.Scores)

	return res
}

func unionKeys[V any](a, b map[string]V) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}

	for k := range b {
		seen[k] = struct{}{}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func compareIntMap(emit func(field, line string), field string, a, b map[string]int) {
	for _, k := range unionKeys(a, b) {
		av, aok := a[k]
		bv, bok := b[k]

		if aok && bok && av == bv {
			continue
		}

		emit(field, fmt.Sprintf("%s[%s]: legacy=%s svp2p=%s", field, k, fmtInt(av, aok), fmtInt(bv, bok)))
	}
}

func compareStringMap(emit func(field, line string), field string, a, b map[string]string) {
	for _, k := range unionKeys(a, b) {
		av, aok := a[k]
		bv, bok := b[k]

		if aok && bok && av == bv {
			continue
		}

		emit(field, fmt.Sprintf("%s[%s]: legacy=%s svp2p=%s", field, k, fmtStr(av, aok), fmtStr(bv, bok)))
	}
}

func fmtInt(v int, ok bool) string {
	if !ok {
		return "<none>"
	}

	return fmt.Sprintf("%d", v)
}

func fmtStr(v string, ok bool) string {
	if !ok {
		return "<none>"
	}

	return v
}

// WriteReport writes the scenario's evidence as markdown into t.TempDir() and,
// when PARITY_REPORT_DIR is set, into that directory too.
func WriteReport(t *testing.T, name string, legacy, svp2p Observation, res Result, transcripts map[Impl][]*svp2ptest.Transcript) {
	t.Helper()

	var b strings.Builder

	fmt.Fprintf(&b, "# parity — %s\n\n", name)
	fmt.Fprintf(&b, "| field | legacy | svp2p |\n|---|---|---|\n")
	fmt.Fprintf(&b, "| BlocksAccepted | %d | %d |\n", legacy.BlocksAccepted, svp2p.BlocksAccepted)
	fmt.Fprintf(&b, "| WallClock | %s | %s |\n", legacy.WallClock.Round(time.Millisecond), svp2p.WallClock.Round(time.Millisecond))
	fmt.Fprintf(&b, "| GetHeadersIn | %d | %d |\n", legacy.GetHeadersIn, svp2p.GetHeadersIn)
	fmt.Fprintf(&b, "| Requests | %v | %v |\n", legacy.Requests, svp2p.Requests)
	fmt.Fprintf(&b, "| Served | %v | %v |\n", legacy.Served, svp2p.Served)
	fmt.Fprintf(&b, "| Disconnected | %v | %v |\n", legacy.Disconnected, svp2p.Disconnected)
	fmt.Fprintf(&b, "| Scores | %v | %v |\n", legacy.Scores, svp2p.Scores)
	fmt.Fprintf(&b, "| Connections | %v | %v |\n", legacy.Connections, svp2p.Connections)
	fmt.Fprintf(&b, "| Notes | %v | %v |\n\n", legacy.Notes, svp2p.Notes)

	fmt.Fprintf(&b, "## Diffs (%d)\n\n", len(res.Diffs))
	for _, d := range res.Diffs {
		fmt.Fprintf(&b, "- %s\n", d)
	}

	fmt.Fprintf(&b, "\n## Accepted divergences (%d)\n\n", len(res.Accepted))
	for _, d := range res.Accepted {
		fmt.Fprintf(&b, "- %s\n", d)
	}

	for _, impl := range []Impl{Legacy, Svp2p} {
		for i, tr := range transcripts[impl] {
			fmt.Fprintf(&b, "\n## %s transcript, peer %d (closed by: %q)\n\n```\n", impl, i, tr.ClosedBy())

			for _, e := range tr.Snapshot() {
				dir := "<<"
				if e.Dir == svp2ptest.Out {
					dir = ">>"
				}

				fmt.Fprintf(&b, "%s %s %s\n", e.At.Format("15:04:05.000"), dir, e.Cmd)
			}

			b.WriteString("```\n")
		}
	}

	file := fmt.Sprintf("parity-%s.md", strings.ReplaceAll(name, "/", "_"))

	require.NoError(t, os.WriteFile(filepath.Join(t.TempDir(), file), []byte(b.String()), 0o600))

	if dir := os.Getenv("PARITY_REPORT_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, file), []byte(b.String()), 0o600))
	}

	t.Logf("parity report: %s (%d diffs, %d accepted)", file, len(res.Diffs), len(res.Accepted))
}
