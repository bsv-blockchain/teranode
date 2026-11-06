package blockvalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_ForkRejectedByHeaderCheckClearsNoMinedSet checks that fork-side mined_set clearing
// (ClearBlockMinedSet plus the BlockMinedUnset notification) does not happen until every
// header-chain check (BV-CATCHUP-007..012, 014) has passed.
//
// Spec: BV-CATCHUP-013 and AC-BV-CATCHUP-013.1 (tracked as GAP-BV-021, resolved).
//
// Scenario: Given a local tip at height 110 and a stored common ancestor at height 100 (fork depth
// 10), and a peer offering 12 headers on the ancestor that pass the fork-depth, secret-mining and
// less-work gates but where header 3 does not point at header 2, When catchup runs, Then it is
// rejected for the broken link and no local block is un-stamped: neither ClearBlockMinedSet nor
// SendNotification is called.
//
// Before bsv-blockchain/teranode#2034 the clearing ran ahead of that check and un-stamped the local
// blocks above the ancestor; it now runs as step 9.6, after every header-chain check.
func TestCatchup_ForkRejectedByHeaderCheckClearsNoMinedSet(t *testing.T) {
	const (
		ancestorMerkleSeed = 0
		ancestorAgeSeconds = 3600
		brokenLinkIndex    = 3
	)

	initPrometheusMetrics()

	// Given: a fork that out-works the local tip but whose header 3 no longer links to header 2.
	ancestorHeader := newCatchupForkStoredHeader(t, ancestorMerkleSeed, ancestorAgeSeconds)
	forkHeaders := testhelpers.CreateSyntheticChainFrom(ancestorHeader, catchupForkHeaderCount)

	bogusParent := chainhash.HashH([]byte("catchup-fork-bogus-parent"))
	forkHeaders[brokenLinkIndex].HashPrevBlock = &bogusParent
	testhelpers.MineHeader(forkHeaders[brokenLinkIndex])

	server, mockBlockchainClient, targetBlock, cleanup := newCatchupForkScenario(t,
		catchupForkAncestorHeight, catchupForkLocalTipHeight, ancestorHeader, forkHeaders)
	defer cleanup()

	// When
	err := server.catchup(context.Background(), targetBlock, catchupForkPeerID, catchupForkPeerURL)

	// Then: the catchup is rejected for the broken link. Since bsv-blockchain/teranode#2034 the
	// header fetch itself rejects a batch that does not link as one chain, so the rejection comes
	// earlier than buildHeaderCache; either way it precedes the clearing.
	requireRejectedForBrokenHeaderLink(t, err)

	// And no local block was un-stamped for a fork that a header check rejected.
	mockBlockchainClient.AssertNotCalled(t, "ClearBlockMinedSet", mock.Anything, mock.Anything)
	mockBlockchainClient.AssertNotCalled(t, "SendNotification", mock.Anything, mock.Anything)
}

// requireRejectedForBrokenHeaderLink asserts the catchup failed because the offered headers do not
// link as one chain.
func requireRejectedForBrokenHeaderLink(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "does not link") || strings.Contains(err.Error(), "chain broken"),
		"catchup must fail on the broken header link, got: %v", err)
}
