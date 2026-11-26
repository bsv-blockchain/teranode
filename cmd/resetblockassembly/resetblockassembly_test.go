package resetblockassembly

import (
	"context"
	"net"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// recordingServer records which reset RPC arrived and the operator header it carried.
type recordingServer struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	called string
	header []string
}

func (r *recordingServer) record(ctx context.Context, name string) (*blockassembly_api.EmptyMessage, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	r.called, r.header = name, md.Get(blockassembly.OperatorResetMetadataKey)

	return &blockassembly_api.EmptyMessage{}, nil
}

func (r *recordingServer) ResetBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	return r.record(ctx, "ResetBlockAssembly")
}

func (r *recordingServer) ResetBlockAssemblyFully(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	return r.record(ctx, "ResetBlockAssemblyFully")
}

func (r *recordingServer) ResetBlockAssemblyValidateInputs(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error) {
	return r.record(ctx, "ResetBlockAssemblyValidateInputs")
}

// Every reset the command sends over gRPC carries the operator header, the only thing that lets
// it run while block assembly is halted on a deep reorg.
func TestResetBlockAssemblySendsOperatorHeader(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	recorder := &recordingServer{}
	srv := grpc.NewServer()
	blockassembly_api.RegisterBlockAssemblyAPIServer(srv, recorder)

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(srv.Stop)

	tSettings := settings.NewSettings()
	tSettings.BlockAssembly.GRPCAddress = lis.Addr().String()

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
		recorder.called, recorder.header = "", nil

		require.NoError(t, ResetBlockAssembly(ulogger.TestLogger{}, tSettings, c.fullReset, c.validateInputs))
		require.Equal(t, c.want, recorder.called)
		require.Equal(t, []string{"true"}, recorder.header)
	}
}
