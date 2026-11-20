package fields

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// fieldNameConstants reads every FieldName constant declared in fields.go, so the check below
// covers fields added later without a list to keep in step.
func fieldNameConstants(t *testing.T) map[string]string {
	t.Helper()

	f, err := parser.ParseFile(token.NewFileSet(), "fields.go", nil, 0)
	require.NoError(t, err)

	out := map[string]string{}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}

		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Values) == 0 {
				continue
			}

			if id, ok := vs.Type.(*ast.Ident); !ok || id.Name != "FieldName" {
				continue
			}

			for i, name := range vs.Names {
				lit, ok := vs.Values[i].(*ast.BasicLit)
				require.True(t, ok, name.Name)

				v, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)

				out[name.Name] = v
			}
		}
	}

	return out
}

// The lane bins are new names in a namespace every store shares: each must be set and must not
// reuse the name of any other field, or a write to one would land in another.
func TestLaneFieldNamesAreDistinct(t *testing.T) {
	all := fieldNameConstants(t)
	require.Equal(t, LanePosition.String(), all["LanePosition"])
	require.Equal(t, SpendingLanePositions.String(), all["SpendingLanePositions"])
	require.Equal(t, ParentLanePositions.String(), all["ParentLanePositions"])

	byValue := map[string]string{}

	for name, v := range all {
		require.NotEmpty(t, v, name)

		if other, ok := byValue[v]; ok {
			// Duplicates that predate the lane fields are not this test's business.
			for _, laneField := range []string{"LanePosition", "SpendingLanePositions", "ParentLanePositions"} {
				require.NotEqual(t, laneField, name, "%s reuses %s's name %q", name, other, v)
				require.NotEqual(t, laneField, other, "%s reuses %s's name %q", other, name, v)
			}
		}

		byValue[v] = name
	}
}
