package resetblockassembly

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// recordingResetter records which reset was called and the operator header its call carried.
type recordingResetter struct {
	called string
	header []string
}

func (r *recordingResetter) record(ctx context.Context, name string) error {
	md, _ := metadata.FromOutgoingContext(ctx)
	r.called, r.header = name, md.Get(blockassembly.OperatorResetMetadataKey)

	return nil
}

func (r *recordingResetter) ResetBlockAssembly(ctx context.Context) error {
	return r.record(ctx, "ResetBlockAssembly")
}

func (r *recordingResetter) ResetBlockAssemblyFully(ctx context.Context) error {
	return r.record(ctx, "ResetBlockAssemblyFully")
}

func (r *recordingResetter) ResetBlockAssemblyValidateInputs(ctx context.Context) error {
	return r.record(ctx, "ResetBlockAssemblyValidateInputs")
}

// Every reset the command makes carries the operator header, the only thing that lets it run
// while block assembly is halted on a deep reorg.
func TestOperatorResetMarksEveryCall(t *testing.T) {
	cases := []struct {
		fullReset, validateInputs bool
		want                      string
	}{
		{want: "ResetBlockAssembly"},
		{fullReset: true, want: "ResetBlockAssemblyFully"},
		{validateInputs: true, want: "ResetBlockAssemblyValidateInputs"},
		{fullReset: true, validateInputs: true, want: "ResetBlockAssemblyValidateInputs"},
	}

	for _, c := range cases {
		ba := &recordingResetter{}
		require.NoError(t, operatorReset(ba, c.fullReset, c.validateInputs))
		require.Equal(t, c.want, ba.called)
		require.Equal(t, []string{"true"}, ba.header)
	}
}
