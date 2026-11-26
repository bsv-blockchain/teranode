package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const pkg = "github.com/bsv-blockchain/teranode/test/consensus"

// goTestJSON builds `go test -json` lines for one package.
func goTestJSON(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

func event(action, test, output string) string {
	s := `{"Action":"` + action + `","Package":"` + pkg + `"`
	if test != "" {
		s += `,"Test":"` + test + `"`
	}

	if output != "" {
		s += `,"Output":` + quote(output)
	}

	return s + "}"
}

func quote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}

	return string(b)
}

func TestReadResults(t *testing.T) {
	t.Run("final outcomes with the last meaningful output", func(t *testing.T) {
		in := goTestJSON(
			"ld: warning: not json",
			event("run", "TestA", ""),
			event("output", "TestA", "=== RUN   TestA\n"),
			event("output", "TestA", "    script_test.go:10: Skipping - go-bdk may not enforce NULLDUMMY\n"),
			event("output", "TestA", "--- SKIP: TestA (0.00s)\n"),
			event("skip", "TestA", ""),
			event("run", "TestB", ""),
			event("pass", "TestB", ""),
			event("run", "TestC", ""),
			event("output", "TestC", "        \tError Trace:\t/x/script_solver_test.go:40\n"),
			event("output", "TestC", "        \tError:      \tShould be true\n"),
			event("output", "TestC", "        \tTest:       \tTestC\n"),
			event("output", "TestC", "--- FAIL: TestC (0.00s)\n"),
			event("fail", "TestC", ""),
			event("run", "TestD", ""),
			event("output", "TestD", "        \tError:      \tShould be true\n"),
			event("output", "TestD", "        \tTest:       \tTestD\n"),
			event("output", "TestD", "        \tMessages:   \tgo-bt does not serialize the transaction back\n"),
			event("fail", "TestD", ""),
			event("pass", "", ""),
		)

		results, err := readResults(strings.NewReader(in))
		require.NoError(t, err)
		require.Equal(t, map[string]outcome{
			pkg + " TestA":     {action: "skip", detail: "script_test.go:10: Skipping - go-bdk may not enforce NULLDUMMY"},
			pkg + " TestB":     {action: "pass"},
			pkg + " TestC":     {action: "fail", detail: "Error:      \tShould be true"},
			pkg + " TestD":     {action: "fail", detail: "Messages:   \tgo-bt does not serialize the transaction back"},
			pkg + " (package)": {action: "pass"},
		}, results)
	})

	t.Run("a build failure is a package failure", func(t *testing.T) {
		in := goTestJSON(
			`{"ImportPath":"`+pkg+` [`+pkg+`.test]","Action":"build-output","Output":"undefined: bt.NewThing\n"}`,
			`{"ImportPath":"`+pkg+` [`+pkg+`.test]","Action":"build-fail"}`,
			event("output", "", "FAIL\t"+pkg+" [build failed]\n"),
			`{"Action":"fail","Package":"`+pkg+`","FailedBuild":"`+pkg+`"}`,
		)

		results, err := readResults(strings.NewReader(in))
		require.NoError(t, err)
		require.Equal(t, map[string]outcome{pkg + " (package)": {action: "fail", detail: "build failed: " + pkg}}, results)
	})

	t.Run("a malformed JSON line is an error", func(t *testing.T) {
		_, err := readResults(strings.NewReader("{not json\n"))
		require.Error(t, err)
	})

	t.Run("every observed package must finish", func(t *testing.T) {
		for _, in := range []string{
			goTestJSON(event("start", "", "")),
			goTestJSON(event("pass", "TestFirst", ""), event("run", "TestLater", "")),
			goTestJSON(event("pass", "", ""), `{"Action":"start","Package":"other"}`),
		} {
			_, err := readResults(strings.NewReader(in))
			require.ErrorContains(t, err, "no final package outcome")
		}
	})
}

func TestDiffResults(t *testing.T) {
	base := map[string]outcome{
		"p TestSame":     {action: "pass"},
		"p TestFlips":    {action: "pass"},
		"p TestToSkip":   {action: "pass"},
		"p TestRemoved":  {action: "pass"},
		"p TestDetail":   {action: "skip", detail: "old reason"},
		"p (package)":    {action: "pass"},
		"p TestFromFail": {action: "fail"},
	}
	head := map[string]outcome{
		"p TestSame":     {action: "pass"},
		"p TestFlips":    {action: "fail", detail: "expected EVAL_FALSE"},
		"p TestToSkip":   {action: "skip", detail: "Skipping - go-bdk behavior differs"},
		"p TestAdded":    {action: "pass"},
		"p TestDetail":   {action: "skip", detail: "new reason"},
		"p (package)":    {action: "fail"},
		"p TestFromFail": {action: "pass"},
	}

	changes, added := diffResults(base, head)

	var keys []string
	for _, c := range changes {
		keys = append(keys, c.key)
	}

	require.Equal(t, []string{"p (package)", "p TestFlips", "p TestFromFail", "p TestRemoved", "p TestToSkip"}, keys,
		"every changed action is reported, sorted, a skip counts as an outcome, and a changed detail alone is not a change")
	require.Equal(t, change{key: "p TestRemoved", base: outcome{action: "pass"}, head: outcome{action: absent}}, changes[3],
		"a test the pull request no longer reports is a change")
	require.Equal(t, []string{"p TestAdded"}, added, "a new test is listed apart and is not a change")
}

func TestWriteReport(t *testing.T) {
	results := map[string]outcome{"p TestA": {action: "pass"}}

	t.Run("no change", func(t *testing.T) {
		var sb strings.Builder
		writeReport(&sb, results, results, nil, nil, 10)
		require.Contains(t, sb.String(), "| base | 1 | 0 | 0 |")
		require.Contains(t, sb.String(), "same outcome in the pull request")
		require.NotContains(t, sb.String(), "new in the pull request")
	})

	t.Run("changes beyond the limit are counted, not dropped", func(t *testing.T) {
		changes := []change{
			{key: "p TestA", base: outcome{action: "pass"}, head: outcome{action: "fail", detail: "a|b `c`"}},
			{key: "p TestB", base: outcome{action: "pass"}, head: outcome{action: "skip"}},
			{key: "p TestC", base: outcome{action: "pass"}, head: outcome{action: "skip"}},
		}

		var sb strings.Builder
		head := map[string]outcome{"p TestA": {action: "pass"}, "p TestNew1": {action: "pass"}, "p TestNew2": {action: "skip"}, "p TestNew3": {action: "pass"}}
		writeReport(&sb, results, head, changes, []string{"p TestNew1", "p TestNew2", "p TestNew3"}, 2)
		out := sb.String()

		require.Contains(t, out, "3 tests or packages have a different outcome")
		require.Contains(t, out, "| `p TestA` | pass | fail: a\\|b 'c' |")
		require.Contains(t, out, "| `p TestB` | pass | skip |")
		require.NotContains(t, out, "TestC`")
		require.Contains(t, out, "1 more not listed; see the job log")
		require.Contains(t, out, "3 tests are new in the pull request")
		require.Contains(t, out, "- `p TestNew2`: skip")
		require.NotContains(t, out, "TestNew3")
		require.Contains(t, out, "1 more not listed.")
	})
}
