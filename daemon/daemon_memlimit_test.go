package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCgroupMemoryValue(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    uint64
		wantErr bool
	}{
		{name: "valid limit", input: "10737418240", want: 10737418240},
		{name: "small limit", input: "1048576", want: 1048576},
		{name: "max means no limit", input: "max", wantErr: true},
		{name: "empty means no limit", input: "", wantErr: true},
		{name: "v1 no limit", input: "9223372036854771712", wantErr: true}, // page-aligned max int64
		{name: "invalid number", input: "abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCgroupMemoryValue(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{input: 1073741824, want: "1.0GiB"},
		{input: 10737418240, want: "10.0GiB"},
		{input: 536870912, want: "512MiB"},
		{input: 1048576, want: "1MiB"},
		{input: 1024, want: "1024B"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, formatBytes(tt.input))
		})
	}
}
