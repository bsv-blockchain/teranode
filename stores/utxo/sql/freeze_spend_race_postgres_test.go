package sql

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

const freezeRaceSpendHeight = 1000

// freezeRacePath names one of the three SQL statements a spend can write with.
type freezeRacePath struct {
	name      string
	batched   bool // BatchSQLOperations: bulk SELECT and UPDATE instead of per-row
	retention bool // block height retention above zero wraps the bulk UPDATE in the DAH CTE
}

var freezeRacePaths = []freezeRacePath{
	{name: "per-row", batched: false, retention: true},
	{name: "bulk with DAH", batched: true, retention: true},
	{name: "bulk without DAH", batched: true, retention: false},
}

// setupFreezeRaceStore stores tests.Tx unspent on Postgres with the spend path chosen.
func setupFreezeRaceStore(t *testing.T, path freezeRacePath) (*Store, context.Context) {
	t.Helper()

	store, ctx := setupPostgresStore(t)

	store.settings.UtxoStore.BatchSQLOperations = path.batched

	if !path.retention {
		store.settings.GlobalBlockHeightRetention = 0
		store.settings.UtxoStore.BlockHeightRetentionAdjustment = 0
	}

	require.Equal(t, path.retention, store.settings.GetUtxoStoreBlockHeightRetention() > 0)

	_, err := store.Create(ctx, tests.Tx, freezeRaceSpendHeight)
	require.NoError(t, err)

	return store, ctx
}

// freezeRaceOutpoint is output 0 of tests.Tx, the coin both sides race for.
func freezeRaceOutpoint(t *testing.T) []*utxo.Spend {
	t.Helper()

	utxoHash, err := util.UTXOHashFromOutput(tests.Tx.TxIDChainHash(), tests.Tx.Outputs[0], 0)
	require.NoError(t, err)

	return []*utxo.Spend{{TxID: tests.Tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash}}
}

// freezeRaceSpendTx spends output 0 of tests.Tx.
func freezeRaceSpendTx(t *testing.T) *bt.Tx {
	t.Helper()

	out := tests.Tx.Outputs[0]

	spendTx := bt.NewTx()
	require.NoError(t, spendTx.From(tests.Tx.TxIDChainHash().String(), 0, out.LockingScript.String(), out.Satoshis))
	require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return spendTx
}

// readFreezeRaceRow returns the frozen flag and the spender of output 0 of tests.Tx.
func readFreezeRaceRow(ctx context.Context, t *testing.T, store *Store) (frozen bool, spendingData []byte) {
	t.Helper()

	hash := tests.Tx.TxIDChainHash()

	require.NoError(t, store.db.QueryRowContext(ctx, `
		SELECT o.frozen, o.spending_data
		FROM outputs o JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1 AND o.idx = 0`, hash[:]).Scan(&frozen, &spendingData))

	return frozen, spendingData
}

// pauseOnce installs fn into hook so the first caller blocks until release, and
// removes it at cleanup.
func pauseOnce(t *testing.T, hook *func()) (reached chan struct{}, release func()) {
	t.Helper()

	reached = make(chan struct{})
	resume := make(chan struct{})

	var once, releaseOnce sync.Once

	*hook = func() {
		once.Do(func() {
			close(reached)
			<-resume
		})
	}

	release = func() { releaseOnce.Do(func() { close(resume) }) }

	t.Cleanup(func() {
		release()

		*hook = nil
	})

	return reached, release
}

func waitReached(t *testing.T, reached chan struct{}, done chan error, what string) {
	t.Helper()

	select {
	case <-reached:
	case err := <-done:
		t.Fatalf("%s returned before its write hook: %v", what, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("%s did not reach its write hook", what)
	}
}

// A freeze that commits after a spend has read the coin as unfrozen, and before the
// spend writes, must stop the spend. Before the fix the spend's write checked only
// that the coin was unspent, so the coin ended up both frozen and spent.
func TestSpendRefusesFreezeCommittedBeforeItsWrite_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, path := range freezeRacePaths {
		t.Run(path.name, func(t *testing.T) {
			store, ctx := setupFreezeRaceStore(t, path)
			outpoint := freezeRaceOutpoint(t)

			reached, release := pauseOnce(t, &spendBeforeWrite)

			var spends []*utxo.Spend

			spendDone := make(chan error, 1)

			go func() {
				var err error
				spends, err = store.Spend(ctx, freezeRaceSpendTx(t), freezeRaceSpendHeight+1)
				spendDone <- err
			}()

			waitReached(t, reached, spendDone, "spend")

			require.NoError(t, store.FreezeUTXOs(ctx, outpoint, store.settings), "nothing has spent the coin yet")

			release()

			err := <-spendDone
			require.Error(t, err, "the spend must not succeed against a committed freeze")
			require.Len(t, spends, 1)
			require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)

			frozen, spendingData := readFreezeRaceRow(ctx, t, store)
			require.True(t, frozen)
			require.Nil(t, spendingData, "a frozen coin must stay unspent")

			resp, err := store.GetSpend(ctx, outpoint[0])
			require.NoError(t, err)
			require.Equal(t, int(utxo.Status_FROZEN), resp.Status)
		})
	}
}

// A spend that commits after a freeze has checked the coin, and before the freeze
// writes, leaves the coin spent. The freeze must then report that it froze nothing;
// before the fix it returned success.
func TestFreezeReportsSpendCommittedBeforeItsWrite_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupFreezeRaceStore(t, freezeRacePaths[0])
	outpoint := freezeRaceOutpoint(t)
	spendTx := freezeRaceSpendTx(t)

	reached, release := pauseOnce(t, &freezeBeforeWrite)

	freezeDone := make(chan error, 1)

	go func() { freezeDone <- store.FreezeUTXOs(ctx, outpoint, store.settings) }()

	waitReached(t, reached, freezeDone, "freeze")

	_, err := store.Spend(ctx, spendTx, freezeRaceSpendHeight+1)
	require.NoError(t, err, "nothing has frozen the coin yet")

	release()

	err = <-freezeDone
	require.ErrorIs(t, err, errors.ErrSpent)

	frozen, spendingData := readFreezeRaceRow(ctx, t, store)
	require.False(t, frozen, "a spent coin must not be marked frozen")
	require.NotNil(t, spendingData)
}

// Freeze and spend released together, many times over, never both win and never leave
// the coin frozen and spent.
func TestFreezeAndSpendNeverBothWin_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	const iterations = 200

	for _, path := range freezeRacePaths {
		t.Run(path.name, func(t *testing.T) {
			store, ctx := setupFreezeRaceStore(t, path)
			outpoint := freezeRaceOutpoint(t)
			spendTx := freezeRaceSpendTx(t)
			hash := tests.Tx.TxIDChainHash()

			outcomes := map[string]int{}

			for i := 0; i < iterations; i++ {
				_, err := store.db.ExecContext(ctx, `
					UPDATE outputs SET frozen = false, spending_data = NULL
					WHERE idx = 0 AND transaction_id = (SELECT id FROM transactions WHERE hash = $1)`, hash[:])
				require.NoError(t, err)

				var (
					start               = make(chan struct{})
					wg                  sync.WaitGroup
					freezeErr, spendErr error
				)

				wg.Add(2)

				go func() {
					defer wg.Done()
					<-start
					freezeErr = store.FreezeUTXOs(ctx, outpoint, store.settings)
				}()

				go func() {
					defer wg.Done()
					<-start
					_, spendErr = store.Spend(ctx, spendTx, freezeRaceSpendHeight+1)
				}()

				close(start)
				wg.Wait()

				frozen, spendingData := readFreezeRaceRow(ctx, t, store)
				require.False(t, frozen && spendingData != nil, "iteration %d: coin is frozen and spent", i)

				switch {
				case freezeErr == nil:
					require.Error(t, spendErr, "iteration %d: freeze and spend both succeeded", i)
					require.True(t, frozen, "iteration %d: freeze succeeded but the coin is not frozen", i)
					require.Nil(t, spendingData, "iteration %d", i)

					outcomes["freeze won"]++
				case spendErr == nil:
					require.ErrorIs(t, freezeErr, errors.ErrSpent, "iteration %d", i)
					require.False(t, frozen, "iteration %d", i)
					require.NotNil(t, spendingData, "iteration %d: spend succeeded but the coin is unspent", i)

					outcomes["spend won"]++
				default:
					t.Fatalf("iteration %d: both failed: freeze %v, spend %v", i, freezeErr, spendErr)
				}
			}

			t.Logf("%s: %v over %d iterations", path.name, outcomes, iterations)
		})
	}
}

// Two copies of one spend in one bulk batch share a single UPDATE row. When a freeze
// stops that row, the copy must fail too, not be reported as an idempotent success.
func TestBulkSpendDuplicateSharesFreezeRefusal_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, path := range freezeRacePaths[1:] {
		t.Run(path.name, func(t *testing.T) {
			store, ctx := setupFreezeRaceStore(t, path)
			outpoint := freezeRaceOutpoint(t)

			spendTx := freezeRaceSpendTx(t)
			item := *outpoint[0]
			item.SpendingData = spendpkg.NewSpendingData(spendTx.TxIDChainHash(), 0)

			batch := make([]*batchSpend, 2)
			for i := range batch {
				batch[i] = &batchSpend{spend: &item, blockHeight: freezeRaceSpendHeight + 1, errCh: make(chan error, 1)}
			}

			reached, release := pauseOnce(t, &spendBeforeWrite)

			retryDone := make(chan error, 1)

			go func() {
				if store.trySendSpendBatchBulk(ctx, batch) {
					retryDone <- errors.NewProcessingError("bulk spend asked for a retry")

					return
				}
				retryDone <- nil
			}()

			waitReached(t, reached, retryDone, "bulk spend")

			require.NoError(t, store.FreezeUTXOs(ctx, outpoint, store.settings))

			release()

			require.NoError(t, <-retryDone)

			for i, b := range batch {
				require.ErrorIs(t, <-b.errCh, errors.ErrFrozen, "batch entry %d", i)
			}

			frozen, spendingData := readFreezeRaceRow(ctx, t, store)
			require.True(t, frozen)
			require.Nil(t, spendingData)
		})
	}
}
