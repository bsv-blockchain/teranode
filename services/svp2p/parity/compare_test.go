package parity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func obs() Observation {
	return Observation{
		BlocksAccepted: 20,
		Requests:       map[string]int{"a": 20},
		Served:         map[string]int{"a": 20},
		Disconnected:   map[string]string{},
		Scores:         map[string]int{},
	}
}

func TestCompare_EqualObservationsHaveNoDiffs(t *testing.T) {
	res := Compare(obs(), obs(), nil)
	require.Empty(t, res.Diffs)
	require.Empty(t, res.Accepted)
}

func TestCompare_ReportsADifferingField(t *testing.T) {
	a, b := obs(), obs()
	b.BlocksAccepted = 19

	res := Compare(a, b, nil)
	require.Equal(t, []string{"BlocksAccepted: legacy=20 svp2p=19"}, res.Diffs)
}

func TestCompare_AnAcceptedDivergenceIsLoggedNotFailed(t *testing.T) {
	a, b := obs(), obs()
	b.BlocksAccepted = 19

	res := Compare(a, b, []Divergence{{Field: "BlocksAccepted", Reason: "legacy single sync peer"}})
	require.Empty(t, res.Diffs)
	require.Equal(t, []string{"BlocksAccepted: legacy=20 svp2p=19 (accepted: legacy single sync peer)"}, res.Accepted)
}

func TestCompare_MapFieldsComparePerKey(t *testing.T) {
	a, b := obs(), obs()
	b.Requests = map[string]int{"a": 12, "b": 8}
	b.Disconnected = map[string]string{"b": "node"}

	res := Compare(a, b, nil)
	require.ElementsMatch(t, []string{
		"Requests[a]: legacy=20 svp2p=12",
		"Requests[b]: legacy=<none> svp2p=8",
		"Disconnected[b]: legacy=<none> svp2p=node",
	}, res.Diffs)
}
