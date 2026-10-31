package alarm

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultRuleDrilldownsResolveToKnownStages(t *testing.T) {
	path := regexp.MustCompile(`^#/stage/(ingress|validator|cache|assembly|chain|stores)$`)
	stage := regexp.MustCompile(`^(ingress|validator|cache|assembly|chain|stores)$`)

	require.NotEmpty(t, DefaultRules)

	for _, r := range DefaultRules {
		require.Regexp(t, stage, r.Stage, "rule %s stage", r.ID)
		require.NotEmpty(t, r.Drilldowns, "rule %s has no drilldowns", r.ID)

		for _, d := range r.Drilldowns {
			require.Regexp(t, path, d.Path, "rule %s drilldown %q", r.ID, d.Label)
		}
	}
}
