package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGRPCReflectionSetting(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{{"false", false}, {"true", true}} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv("grpc_enable_reflection", tc.value)
			require.Equal(t, tc.want, NewSettings().GRPCEnableReflection)
		})
	}
}
