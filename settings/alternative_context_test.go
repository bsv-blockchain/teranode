package settings

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// A value set only under an alternative context must reach the loaded
// settings when NewSettings is called with that context.
func TestNewSettings_AlternativeContextDurations(t *testing.T) {
	const altContext = "alternativecontexttest"

	tests := []struct {
		key   string
		value time.Duration
		got   func(s *Settings) time.Duration
	}{
		{
			key:   "p2p_ban_duration",
			value: 7 * time.Minute,
			got:   func(s *Settings) time.Duration { return s.P2P.BanDuration },
		},
		{
			key:   "blockvalidation_check_subtree_from_block_timeout",
			value: 11 * time.Second,
			got:   func(s *Settings) time.Duration { return s.BlockValidation.CheckSubtreeFromBlockTimeout },
		},
		{
			key:   "blockvalidation_check_subtree_from_block_retry_backoff_duration",
			value: 13 * time.Second,
			got:   func(s *Settings) time.Duration { return s.BlockValidation.CheckSubtreeFromBlockRetryBackoffDuration },
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			// gocore caches a snapshot of the config per alternative context,
			// so the value is set on that context's own configuration.
			ctxKey := tt.key + "." + altContext
			gocore.Config(altContext).Set(ctxKey, tt.value.String())
			t.Cleanup(func() { gocore.Config(altContext).Unset(ctxKey) })

			require.Equal(t, tt.value, tt.got(NewSettings(altContext)), "%s must be read under context %q", tt.key, altContext)
			require.NotEqual(t, tt.value, tt.got(NewSettings()), "%s must not leak into the default context", tt.key)
		})
	}
}

// Every settings getter called in NewSettings must forward alternativeContext,
// otherwise that key silently ignores NewSettings(context).
func TestNewSettings_EveryGetterForwardsAlternativeContext(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "settings.go", nil, 0)
	require.NoError(t, err)

	var newSettings *ast.FuncDecl

	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "NewSettings" {
			newSettings = fn
		}
	}

	require.NotNil(t, newSettings, "NewSettings not found in settings.go")

	var missing []string

	ast.Inspect(newSettings.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		fn, ok := call.Fun.(*ast.Ident)
		if !ok || !strings.HasPrefix(fn.Name, "get") || len(call.Args) == 0 {
			return true
		}

		last, ok := call.Args[len(call.Args)-1].(*ast.Ident)
		if !ok || last.Name != "alternativeContext" || !call.Ellipsis.IsValid() {
			missing = append(missing, fset.Position(call.Pos()).String()+" "+fn.Name)
		}

		return true
	})

	require.Empty(t, missing, "settings getters that do not forward alternativeContext...")
}
