package teranode

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunDaemonUsesTheRedactedConfigHelpers pins the two RunDaemon call sites
// the redaction tests cannot reach. RunDaemon starts the whole node, so no test
// calls it, and gocore keeps its registered payload functions unexported, so a
// test cannot read back what was registered. The tests in
// config_dump_redaction_test.go call the helpers directly. Without this one,
// putting gocore.Config().Stats() back into the boot log line, or registering
// gocore.Config().GetAll as the payload, leaves every test green.
//
// It reads the package source: every zero-argument Stats() or GetAll() call
// must sit inside the helper that redacts it, every payload registration must
// pass configAdvertisingPayload, and RunDaemon must call redactedConfigDump.
func TestRunDaemonUsesTheRedactedConfigHelpers(t *testing.T) {
	fset := token.NewFileSet()

	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)

	pkg, ok := pkgs["teranode"]
	require.True(t, ok, "package teranode not found in cmd/teranode")

	// The only function allowed to read each raw gocore view.
	allowedIn := map[string]string{
		"Stats":  "redactedConfigDump",
		"GetAll": "configAdvertisingPayload",
	}

	dumpCalledFromRunDaemon := false
	payloadRegistrations := 0
	rawReads := map[string]int{}

	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}

				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if helper, policed := allowedIn[fun.Sel.Name]; policed && len(call.Args) == 0 {
						require.Equal(t, helper, fn.Name.Name,
							"%s: %s() is read in %s; the raw gocore config may only be read inside %s, which redacts it",
							fset.Position(call.Pos()), fun.Sel.Name, fn.Name.Name, helper)

						rawReads[fun.Sel.Name]++
					}

					if fun.Sel.Name == "AddAppPayloadFn" {
						require.Len(t, call.Args, 2, "%s: unexpected AddAppPayloadFn arity", fset.Position(call.Pos()))

						arg, isIdent := call.Args[1].(*ast.Ident)
						require.True(t, isIdent && arg.Name == "configAdvertisingPayload",
							"%s: the advertising payload must be configAdvertisingPayload, which redacts gocore's map",
							fset.Position(call.Pos()))

						payloadRegistrations++
					}
				case *ast.Ident:
					if fun.Name == "redactedConfigDump" && fn.Name.Name == "RunDaemon" {
						dumpCalledFromRunDaemon = true
					}
				}

				return true
			})
		}
	}

	require.True(t, dumpCalledFromRunDaemon, "RunDaemon no longer logs redactedConfigDump()")
	require.Equal(t, 1, payloadRegistrations, "RunDaemon should register exactly one advertising payload")

	// If a helper stops reading gocore, the rules above police nothing.
	require.Equal(t, 1, rawReads["Stats"], "redactedConfigDump no longer reads gocore's Stats(), so this pin is stale")
	require.Equal(t, 1, rawReads["GetAll"], "configAdvertisingPayload no longer reads gocore's GetAll(), so this pin is stale")
}
