package urlutil_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// This guard lives next to urlutil.Redact because it is what makes the helper
// load-bearing: without it, the next URL logged unredacted reintroduces the
// defect that motivated the helper (Teranode store URLs carry credentials in
// their userinfo, so a URL in a log line is a working credential in a log
// line).
//
// It is deliberately syntax-only. It reads names, not types, so it is fast
// enough to run in `make test`. The trade is that it catches the realistic
// regression - somebody writing logger.Infof("...", storeURL) - and not a URL
// laundered through a variable named something else. A type-aware version
// would need to load and type-check the whole module, which takes minutes.

// loggingCalls are the call names whose arguments reach an operator: log
// methods, the fmt printers, and the teranode error constructors, whose
// messages are logged and can reach an API response.
//
// Sprintf, Sprint and Sprintln are here because formatting a URL into a string
// and logging that string is the common laundering path: the URL reaches the
// operator just the same, but the logging call it reaches them through only
// sees an already-built string.
var loggingCalls = regexp.MustCompile(`^(Debugf|Infof|Warnf|Errorf|Fatalf|Panicf|Print|Printf|Println|Sprint|Sprintf|Sprintln|Fprint|Fprintf|Fprintln|New[A-Za-z]*Error)$`)

// urlish matches an argument expression that names itself as a URL.
var urlish = regexp.MustCompile(`(?i)(url|dsn|connstr)`)

// storeSetting matches a settings field reached through a settings value whose
// name does not say URL, such as appSettings.UtxoStore.UtxoStore or
// tSettings.Kafka.InvalidBlocksConfig. Most configured store and Kafka URLs
// are named for what they point at, so urlish alone cannot see them.
//
// Address and Addresses are here because some settings that hold a full URL
// are named for the address, such as asset_propagation_proxy_address and
// propagation_httpAddresses, and userinfo in a URL is a working credential.
// Listen addresses match too and carry a urlsafe escape.
var storeSetting = regexp.MustCompile(`(?i)settings\.[A-Za-z0-9_.]*(store|config|address|addresses)(\.String\(\))?$`)

// viaSettings matches an argument reached through a settings value, which is
// this node's own configuration even inside a peer-URL package.
var viaSettings = regexp.MustCompile(`(?i)settings\.`)

// redacted matches an argument that has already been through a redacting
// helper, either this package's or the standard library's, or that is
// urlutil.ParseErrorReason, whose output is fixed text that quotes no input.
var redacted = regexp.MustCompile(`urlutil\.Redact|urlutil\.ParseErrorReason\(|\.Redacted\(\)`)

// safeAccessors are URL components that carry no credential, so logging them
// raw is fine. Hostname and Port are methods; Scheme, Host, Path and Opaque
// are fields. Host keeps a port but never userinfo.
var safeAccessors = []string{
	".Scheme", ".Host", ".Hostname()", ".Port()", ".Path", ".Opaque", ".RequestURI()",
}

// peerURLPackages hold URLs supplied by remote peers over gossip and HTTP,
// not URLs read from this node's own configuration. A peer's announced
// DataHubURL or baseURL holds no credential of ours, and these packages log
// them constantly while diagnosing sync and catch-up. Redacting them would be
// churn with nothing behind it.
//
// The exemption covers URL-named arguments only. An argument reached through a
// settings value is this node's configuration, so it is still policed here, as
// in logger.Infof("%s", tSettings.Kafka.InvalidSubtreesConfig).
var peerURLPackages = []string{
	"services/blockvalidation/",
	"services/subtreevalidation/",
	"services/p2p/",
	"model/",
}

// skipDirs are trees this guard does not police: third-party code, fixtures,
// the dashboard, and the integration harness under test/, which builds URLs
// for containers it started itself.
var skipDirs = map[string]bool{
	".git": true, ".claude": true, ".worktrees": true,
	"vendor": true, "testdata": true, "node_modules": true,
	"ui": true, "test": true,
}

// escapeComment marks a flagged line as reviewed. It must carry a reason:
//
//	logger.Infof("store: %s", cfg.StoreURL) // urlsafe: redacted at source in X
const escapeComment = "urlsafe:"

type violation struct {
	pos  string
	call string
	arg  string
}

func TestNoUnredactedURLsInLogCalls(t *testing.T) {
	root := repoRoot(t)

	var violations []violation

	// A repo walk that silently finds nothing would make this test pass while
	// policing nothing at all, so count what was actually scanned.
	scanned := 0

	fset := token.NewFileSet()

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil //nolint:nilerr // an unreadable tree is not this guard's business
		}

		if d.IsDir() {
			if skipDirs[d.Name()] {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		rel := filepath.ToSlash(mustRel(t, root, path))

		peerPkg := false

		for _, pkg := range peerURLPackages {
			if strings.HasPrefix(rel, pkg) {
				peerPkg = true
				break
			}
		}

		src, readErr := os.ReadFile(path) //nolint:gosec // walking a known repo tree
		if readErr != nil {
			return nil
		}

		file, parseErr := parser.ParseFile(fset, path, src, parser.ParseComments)
		if parseErr != nil {
			return nil
		}

		scanned++

		lines := strings.Split(string(src), "\n")
		boundLoggers := loggingFuncVars(file)

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			callName, ok := loggingCallName(call.Fun, boundLoggers)
			if !ok {
				return true
			}

			formatIdx := formatArgIndex(callName)

			for i, arg := range call.Args {
				// The format string names no variable when it is a literal, so
				// skip it. One built by concatenation, as in
				// errors.NewConfigurationError("bad URL "+u.String(), err),
				// carries its operands into the message and is policed.
				if _, literal := arg.(*ast.BasicLit); i == formatIdx && literal {
					continue
				}

				text := exprText(arg)
				if !isConfiguredURL(text, peerPkg) || redacted.MatchString(text) || isSafeAccessor(text) {
					continue
				}

				argLine := fset.Position(arg.Pos()).Line
				callLine := fset.Position(call.Pos()).Line

				if hasEscape(lines, argLine) || hasEscape(lines, callLine) || hasEscape(lines, callLine-1) {
					continue
				}

				p := fset.Position(arg.Pos())

				violations = append(violations, violation{
					pos:  fmt.Sprintf("%s:%d:%d", rel, p.Line, p.Column),
					call: callName,
					arg:  text,
				})
			}

			return true
		})

		return nil
	})
	require.NoError(t, err)

	// The guard reaches roughly 830 non-test .go files outside the exempt
	// trees. A floor of 500 was too slack to bite: services/ alone is about
	// 370 files, so the guard could stop seeing every service in the repo and
	// still clear it. 700 leaves headroom for the tree shrinking a little
	// without letting a whole subtree drop out unnoticed.
	require.Greater(t, scanned, 700, "the guard scanned almost nothing, so it proves nothing")

	sort.Slice(violations, func(i, j int) bool { return violations[i].pos < violations[j].pos })

	if len(violations) == 0 {
		return
	}

	report := make([]string, 0, len(violations))
	for _, v := range violations {
		report = append(report, "  "+v.pos+": "+v.call+"(..., "+v.arg+")")
	}

	t.Fatalf("URL passed to a logging call without redaction:\n%s\n\n"+
		"Teranode store URLs carry credentials in their userinfo, so a URL in a log line is a\n"+
		"working credential in a log line. Wrap the argument in urlutil.Redact (for a *url.URL)\n"+
		"or urlutil.RedactString (for a string), or log only .Scheme/.Host/.Path.\n"+
		"If the value is genuinely safe, add a trailing comment saying why:\n"+
		"    // %s <reason>", strings.Join(report, "\n"), escapeComment)
}

// loggingCallName reports whether a call's function is a logging call, and
// names it for the report. Most are direct selector calls such as
// logger.Infof or errors.NewServiceError. A call through a function variable
// bound to one of those, as in errFn := errors.NewServiceError followed by
// errFn("...", rawURL), is policed too, because otherwise choosing the error
// constructor at runtime hides the argument from the guard.
func loggingCallName(fun ast.Expr, boundLoggers map[string]string) (string, bool) {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name, loggingCalls.MatchString(f.Sel.Name)
	case *ast.Ident:
		if bound, ok := boundLoggers[f.Name]; ok {
			return f.Name + " (" + bound + ")", true
		}
	}

	return "", false
}

// fprintCalls write to an io.Writer, so their first argument is the writer and
// the format string is the second.
var fprintCalls = regexp.MustCompile(`^Fprint(f|ln)?$`)

// formatArgIndex reports which argument holds the format string, so a literal
// one can be skipped without also skipping a real argument. The name may carry
// the " (bound to X)" suffix loggingCallName adds for a function value.
func formatArgIndex(callName string) int {
	if fprintCalls.MatchString(strings.SplitN(callName, " ", 2)[0]) {
		return 1
	}

	return 0
}

// loggingFuncVars collects the names of variables in a file that are assigned
// a logging call as a function value, such as errFn := errors.NewServiceError.
// It matches by name, not by scope, so a same-named variable in another
// function of the same file is treated alike. That can only add a report,
// never hide one. A variable declared without a value and assigned later,
// var errFn func(...) then errFn = errors.NewServiceError, is caught by the
// later assignment.
func loggingFuncVars(file *ast.File) map[string]string {
	bound := map[string]string{}

	record := func(lhs, rhs ast.Expr) {
		id, ok := lhs.(*ast.Ident)
		if !ok {
			return
		}

		sel, ok := rhs.(*ast.SelectorExpr)
		if !ok || !loggingCalls.MatchString(sel.Sel.Name) {
			return
		}

		// Keep the first binding so the report names the initial constructor.
		if _, seen := bound[id.Name]; !seen {
			bound[id.Name] = sel.Sel.Name
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch s := n.(type) {
		case *ast.AssignStmt:
			if len(s.Lhs) == len(s.Rhs) {
				for i := range s.Lhs {
					record(s.Lhs[i], s.Rhs[i])
				}
			}
		case *ast.ValueSpec:
			if len(s.Names) == len(s.Values) {
				for i := range s.Names {
					record(s.Names[i], s.Values[i])
				}
			}
		}

		return true
	})

	return bound
}

// isConfiguredURL reports whether an argument names a URL this guard polices.
// In a peer-URL package a URL-named argument is a peer's URL unless it is
// reached through a settings value, so only settings paths count there.
func isConfiguredURL(text string, peerPkg bool) bool {
	if storeSetting.MatchString(text) {
		return true
	}

	if !urlish.MatchString(text) {
		return false
	}

	return !peerPkg || viaSettings.MatchString(text)
}

func hasEscape(lines []string, line int) bool {
	if line < 1 || line > len(lines) {
		return false
	}

	idx := strings.Index(lines[line-1], escapeComment)
	if idx < 0 {
		return false
	}

	// The escape must carry a reason, so it cannot be pasted in blank.
	return strings.TrimSpace(lines[line-1][idx+len(escapeComment):]) != ""
}

func isSafeAccessor(text string) bool {
	for _, s := range safeAccessors {
		if strings.HasSuffix(text, s) {
			return true
		}
	}

	return false
}

// exprText renders the shape of an argument expression. It is intentionally
// lossy: the guard only needs the identifier names to decide whether the
// argument calls itself a URL.
func exprText(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name
	case *ast.SelectorExpr:
		return exprText(v.X) + "." + v.Sel.Name
	case *ast.CallExpr:
		return exprText(v.Fun) + "()"
	case *ast.IndexExpr:
		return exprText(v.X) + "[]"
	case *ast.StarExpr:
		return "*" + exprText(v.X)
	case *ast.UnaryExpr:
		return exprText(v.X)
	case *ast.ParenExpr:
		return exprText(v.X)
	case *ast.BasicLit:
		// A literal's own text counts. Without this, the message in
		// logger.Infof("store URL " + storeCfg.String()) is dropped and the
		// word that identifies it as a URL goes with it. Argument 0 of a call
		// is the format string and is skipped by the caller, so the false
		// positives this can add are literals in later argument positions,
		// which the "// urlsafe:" escape already covers.
		return v.Value
	case *ast.BinaryExpr:
		return exprText(v.X) + "+" + exprText(v.Y)
	default:
		return ""
	}
}

func mustRel(t *testing.T, root, path string) string {
	t.Helper()

	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}

	return rel
}

// repoRoot walks up from the test's directory to the module root. It skips the
// test rather than failing when this package is consumed from outside a
// teranode checkout, where there is no repo to police.
func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	require.NoError(t, err)

	for {
		mod := filepath.Join(dir, "go.mod")
		if b, readErr := os.ReadFile(mod); readErr == nil { //nolint:gosec // walking up from the test's own directory
			if strings.Contains(string(b), "module github.com/bsv-blockchain/teranode") {
				return dir
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			t.Skip("not running inside a teranode checkout")
		}

		dir = parent
	}
}
