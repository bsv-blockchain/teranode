package daemon

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// daemonPackageDir is the directory scanned for d.shouldStart(...) call sites,
// which define the authoritative set of services the daemon can start. Scanning
// the whole package (rather than just daemon_services.go) catches dispatch calls
// that live elsewhere, such as daemon.go's "wait_for_postgres" switch.
const daemonPackageDir = "."

// daemonReceiverIdent is the receiver name a call must use to be treated as a
// daemon method call (e.g. d.shouldStart(...)), so that an unrelated same-named
// method or field on another type in the package isn't folded into the inventory.
const daemonReceiverIdent = "d"

// daemonServiceNames maps the identifier of every "Formal" service-name constant
// that startServices() dispatches to that constant's value. The identifiers are
// cross-checked against the real shouldStart() call sites in the daemon package, so
// this map cannot drift from the code in either direction.
var daemonServiceNames = map[string]string{
	"serviceAlertFormal":             serviceAlertFormal,
	"serviceAssetFormal":             serviceAssetFormal,
	"serviceBlockAssemblyFormal":     serviceBlockAssemblyFormal,
	"serviceBlockPersisterFormal":    serviceBlockPersisterFormal,
	"serviceBlockValidationFormal":   serviceBlockValidationFormal,
	"serviceBlockchainFormal":        serviceBlockchainFormal,
	"serviceLegacyFormal":            serviceLegacyFormal,
	"serviceNameP2PFormal":           serviceNameP2PFormal,
	"servicePropagationFormal":       servicePropagationFormal,
	"servicePrunerFormal":            servicePrunerFormal,
	"serviceRPCFormal":               serviceRPCFormal,
	"serviceSubtreeValidationFormal": serviceSubtreeValidationFormal,
	"serviceUtxoPersisterFormal":     serviceUtxoPersisterFormal,
	"serviceValidatorFormal":         serviceValidatorFormal,
}

// nonServiceShouldStartArgs holds shouldStart() arguments that are command-line
// switches rather than services, and so are exempt from the inventory. Both
// identifier names (e.g. serviceHelp) and string-literal switches (e.g.
// wait_for_postgres) are looked up here, keyed by their literal text.
var nonServiceShouldStartArgs = map[string]struct{}{
	// -help only prints usage; it starts nothing and has no services/ package.
	"serviceHelp": {},
	// wait_for_postgres (daemon.go) is a startup gate, not a service.
	"wait_for_postgres": {},
}

// TestServiceInventory_MatchesServicesDirectory guards the daemon's service dispatch
// against the services/ layout in both directions: the set of services
// startServices() can dispatch is derived from the actual shouldStart() call sites
// in the daemon package, so a service added to (or removed from) that dispatch
// without being added to (or removed from) daemonServiceNames fails the test. Every
// inventoried service must also have a source package under services/<name>. This
// does not check the architecture docs — those still need a manual pass.
func TestServiceInventory_MatchesServicesDirectory(t *testing.T) {
	dispatched := shouldStartServiceIdents(t)

	// Direction 1: nothing the daemon dispatches may be missing from the inventory.
	for ident := range dispatched {
		_, listed := daemonServiceNames[ident]
		require.True(t, listed,
			"daemon package dispatches shouldStart(%s) but %s is missing from daemonServiceNames: add it here and add its services/<name> package",
			ident, ident)
	}

	// Direction 2: nothing in the inventory may be a service the daemon no longer
	// dispatches.
	for ident := range daemonServiceNames {
		_, ok := dispatched[ident]
		require.True(t, ok,
			"daemonServiceNames lists %s but the daemon package never passes it to shouldStart: remove it here or restore the dispatch",
			ident)
	}

	// Every dispatched service must have a source package under services/<name>.
	servicesDir := filepath.Join("..", "services")

	for ident, name := range daemonServiceNames {
		dirName := strings.ToLower(name)
		pkgPath := filepath.Join(servicesDir, dirName)

		info, err := os.Stat(pkgPath)
		require.NoError(t, err, "daemon can start service %q (%s) but services/%s does not exist", name, ident, dirName)
		require.True(t, info.IsDir(), "services/%s exists but is not a directory", dirName)
	}
}

// shouldStartServiceIdents parses every non-test .go file in daemonPackageDir and
// returns the identifiers passed as the first argument to every
// daemonReceiverIdent.shouldStart(...) call, minus the non-service switches.
// Deriving the set from the source rather than restating it keeps the guard
// bidirectional. Scanning the whole package (rather than just daemon_services.go)
// catches dispatch calls that live elsewhere. shouldStart must be called directly
// (e.g. d.shouldStart(...)); a method value taken as ss := d.shouldStart and
// invoked as ss(...) is not visible to this AST match.
func shouldStartServiceIdents(t *testing.T) map[string]struct{} {
	t.Helper()

	fset := token.NewFileSet()

	entries, err := os.ReadDir(daemonPackageDir)
	require.NoError(t, err, "failed to read %s", daemonPackageDir)

	idents := make(map[string]struct{})
	found := false

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		path := filepath.Join(daemonPackageDir, name)

		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err, "failed to parse %s", path)

		ast.Inspect(file, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "shouldStart" || len(call.Args) == 0 {
				return true
			}

			recv, ok := sel.X.(*ast.Ident)
			if !ok || recv.Name != daemonReceiverIdent {
				return true
			}

			found = true

			switch arg := call.Args[0].(type) {
			case *ast.Ident:
				if _, exempt := nonServiceShouldStartArgs[arg.Name]; exempt {
					return true
				}

				idents[arg.Name] = struct{}{}
			case *ast.BasicLit:
				lit := strings.Trim(arg.Value, `"`)
				_, exempt := nonServiceShouldStartArgs[lit]
				require.True(t, exempt,
					"shouldStart(%s) at %s is not in nonServiceShouldStartArgs: add it there if it isn't a service",
					arg.Value, fset.Position(call.Lparen))
			default:
				t.Fatalf("shouldStart is called with an unrecognized argument kind at %s: this guard can only inventory constant identifiers and string literals", fset.Position(call.Lparen))
			}

			return true
		})
	}

	require.True(t, found, "found no %s.shouldStart call sites in %s: has the service dispatch moved?", daemonReceiverIdent, daemonPackageDir)
	require.NotEmpty(t, idents, "found no service identifiers among %s.shouldStart call sites in %s", daemonReceiverIdent, daemonPackageDir)

	return idents
}

// settingsConfPath is settings.conf relative to the daemon package directory.
const settingsConfPath = "../settings.conf"

// startKeyPattern matches a settings.conf base key with the shape shouldStart's
// fmt.Sprintf("start%s", app) construction produces for a real service:
// "start" followed by an uppercase letter. CLI-only switches dispatched
// through shouldStart with a lowercase argument (help, wait_for_postgres)
// never match this shape.
var startKeyPattern = regexp.MustCompile(`^start([A-Z][A-Za-z0-9]*)$`)

// nonServiceStartConfigKeys documents the settings.conf keys that shouldStart's
// CLI-only, non-service arguments would produce if they were ever set via
// config instead of a command-line flag. Neither currently appears in
// settings.conf, but they are excluded here explicitly - rather than relying
// on startKeyPattern's uppercase rule alone - so a future rename of one of
// these switches to an uppercase-leading spelling fails loudly here instead of
// silently joining the service inventory.
var nonServiceStartConfigKeys = map[string]struct{}{
	"starthelp":              {},
	"startwait_for_postgres": {},
}

// settingsConfStartKeys parses settings.conf directly (not via gocore, so the
// guard cannot be defeated by a gocore singleton already loaded with a
// different context elsewhere in the test binary) and returns the set of
// distinct base keys - context suffixes such as ".operator" stripped -
// matching startKeyPattern.
func settingsConfStartKeys(t *testing.T) map[string]struct{} {
	t.Helper()

	f, err := os.Open(settingsConfPath)
	require.NoError(t, err, "failed to open %s", settingsConfPath)

	defer f.Close()

	keys := make(map[string]struct{})

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()

		line, _, _ = strings.Cut(line, "#")

		rawKey, _, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key := strings.TrimSpace(rawKey)
		if key == "" {
			continue
		}

		// Strip any context suffix (e.g. "startP2P.operator" -> "startP2P").
		if i := strings.Index(key, "."); i >= 0 {
			key = key[:i]
		}

		if _, exempt := nonServiceStartConfigKeys[key]; exempt {
			continue
		}

		if startKeyPattern.MatchString(key) {
			keys[key] = struct{}{}
		}
	}

	require.NoError(t, scanner.Err(), "failed to scan %s", settingsConfPath)

	return keys
}

// TestServiceInventory_MatchesSettingsConf extends the daemonServiceNames
// cross-check to settings.conf: every "^start[A-Z]" key in the base config
// must correspond to a real shouldStart() dispatch, and every dispatched
// service must have a matching "start<Formal>" key in settings.conf. This is
// the check that would have caught startFaucet/startCoinbase lingering in
// settings.conf for services the daemon can no longer start.
//
// shouldStart builds the config key as "start" + the formal name verbatim
// (fmt.Sprintf("start%s", app)), and gocore is case-sensitive, so this is a
// byte-for-byte match, not a case-insensitive one: if a formal name and its
// settings.conf key ever drift apart in case, this test is what catches it.
func TestServiceInventory_MatchesSettingsConf(t *testing.T) {
	confKeys := settingsConfStartKeys(t)

	wantKeys := make(map[string]struct{}, len(daemonServiceNames))
	for _, formal := range daemonServiceNames {
		wantKeys["start"+formal] = struct{}{}
	}

	// Direction 1: every "start<X>" key in settings.conf must be a service the
	// daemon actually dispatches.
	for key := range confKeys {
		_, ok := wantKeys[key]
		require.True(t, ok,
			"settings.conf defines %q but no daemon shouldStart() call site dispatches it: "+
				"remove the key from settings.conf, or add the service to daemonServiceNames "+
				"and daemon_services.go if it is real", key)
	}

	// Direction 2: every service the daemon dispatches must have a settings.conf key.
	for key := range wantKeys {
		_, ok := confKeys[key]
		require.True(t, ok,
			"daemon dispatches a service expecting settings.conf key %q, but it is missing from "+
				"settings.conf: operators have no way to enable/disable it via config", key)
	}
}
