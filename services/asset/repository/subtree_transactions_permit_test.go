package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestGetSubtreeTransactionsHoldsPermitUntilRelease pins the permit contract:
// asset_concurrency_get_subtree_transactions budgets the transaction map, not
// the few milliseconds spent building it. The permit used to be released on
// return while the caller kept the whole map alive for the duration of the
// request, so the configured concurrency bounded nothing.
func TestGetSubtreeTransactionsHoldsPermitUntilRelease(t *testing.T) {
	_, subtreeHash, repo := setupSubtreeData(t)

	// test.CreateBaseTestSettings ships asset_concurrency_get_subtree_transactions = 2.
	const permits = 2

	releases := make([]func(), 0, permits)

	for i := 0; i < permits; i++ {
		txMap, release, err := repo.GetSubtreeTransactions(context.Background(), subtreeHash)
		require.NoError(t, err)
		require.NotNil(t, release)
		require.Len(t, txMap, 2)

		releases = append(releases, release)
	}

	// Every permit is held by a map the caller still owns, so the next request
	// must queue rather than build a third map.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, release, err := repo.GetSubtreeTransactions(ctx, subtreeHash)
	require.Error(t, err, "a third concurrent map must not be built while both permits are held")
	require.NotNil(t, release, "release must be safe to call on the error path")

	release()

	for _, r := range releases {
		r()
	}

	// Once released, the budget is available again.
	txMap, release, err := repo.GetSubtreeTransactions(context.Background(), subtreeHash)
	require.NoError(t, err)
	require.Len(t, txMap, 2)

	release()
}
