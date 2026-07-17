package blockassembly

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGenerateEmptyBlockCandidateUsesSafeVersion proves that the empty mining candidate does not
// inherit the tip's block version. Inheriting an old version at an activation boundary would
// make Teranode mine a block below the new floor that it (and the network) must reject. The tip
// here is genesis (v1); the candidate must nevertheless carry the safe 0x20000000 version, matching
// the main mining candidate path.
func TestGenerateEmptyBlockCandidateUsesSafeVersion(t *testing.T) {
	server, _ := setupServer(t)
	require.NoError(t, server.blockAssembler.Start(t.Context()))

	bestHeader, meta, err := server.blockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.NotNil(t, bestHeader)

	candidate, _, err := server.blockAssembler.generateEmptyBlockCandidate(bestHeader, meta.Height)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.Equal(t, uint32(0x20000000), candidate.Version)
}
