package replayrecovery

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

type recordBackend struct {
	records       map[string]Record
	txs           map[string]*bt.Tx
	seeds         []string
	writes        int
	failMark      bool
	failAfterMark bool
	failDeleteKey string
}

func (b *recordBackend) Identity() string { return "test-store" }
func (b *recordBackend) Scan(ctx context.Context, emit func(string) error) error {
	for _, id := range b.seeds {
		if err := emit(id); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (b *recordBackend) Inputs(_ context.Context, id string) ([]string, error) {
	tx := b.txs[id]
	if tx == nil {
		return nil, nil
	}
	out := []string{}
	for _, in := range tx.Inputs {
		out = append(out, in.PreviousTxIDChainHash().String())
	}
	return out, nil
}
func (b *recordBackend) Read(_ context.Context, key []byte) (*Record, error) {
	r, ok := b.records[string(key)]
	if !ok {
		return nil, nil
	}
	return &r, nil
}
func (b *recordBackend) Equal(a, c Record) bool {
	return a.Generation == c.Generation && string(a.Data) == string(c.Data) && a.ExpiresAt == c.ExpiresAt && string(a.Key) == string(c.Key)
}
func (b *recordBackend) HasMarker(r Record, child string) bool {
	return string(r.Data) == "spent:"+child+":marked"
}
func (b *recordBackend) MatchesMarked(a, c Record, child string) bool {
	return c.Generation == a.Generation+1 && c.ExpiresAt == a.ExpiresAt && string(a.Data) == "spent:"+child && b.HasMarker(c, child)
}
func (b *recordBackend) Snapshot(_ context.Context, tx *bt.Tx) (Snapshot, error) {
	id := tx.TxID()
	child, ok := b.records[id]
	if !ok {
		return Snapshot{}, failure("missing child")
	}
	parent := b.records["parent"]
	records := []Record{child}
	if page, ok := b.records[id+":page"]; ok {
		records = append(records, page)
	}
	return Snapshot{TxID: id, Records: records, Parents: []Parent{{Record: parent, Child: id}}}, nil
}
func (b *recordBackend) Mark(_ context.Context, r Record, child string) (Record, error) {
	b.writes++
	if b.failMark {
		return Record{}, failure("marker failed")
	}
	cur, ok := b.records[string(r.Key)]
	if !ok || !b.Equal(r, cur) {
		return Record{}, failure("generation conflict")
	}
	r.Generation++
	r.Data = []byte("spent:" + child + ":marked")
	b.records[string(r.Key)] = r
	if b.failAfterMark {
		return Record{}, failure("ambiguous marker response")
	}
	return r, nil
}
func (b *recordBackend) Delete(_ context.Context, r Record) error {
	b.writes++
	if string(r.Key) == b.failDeleteKey {
		return failure("page deletion failed")
	}
	cur, ok := b.records[string(r.Key)]
	if !ok || !b.Equal(r, cur) {
		return failure("generation conflict")
	}
	delete(b.records, string(r.Key))
	return nil
}

func (b *recordBackend) VerifyParent(_ context.Context, p Parent) error {
	r, ok := b.records[string(p.Record.Key)]
	if !ok || !b.HasMarker(r, p.Child) {
		return failure("parent spend changed")
	}
	return nil
}

type evidenceSource struct {
	tip      Tip
	evidence map[string]Evidence
}

func (s *evidenceSource) Tip(context.Context) (Tip, error) { return s.tip, nil }
func (s *evidenceSource) Check(_ context.Context, id string, tip Tip) (Evidence, error) {
	e, ok := s.evidence[id]
	if !ok {
		return Evidence{TxID: id, Tip: tip, Classification: Unknown, Reason: "not found"}, nil
	}
	e.Tip = tip
	return e, nil
}

func recoveryFixture(t *testing.T) (*recordBackend, *evidenceSource, *bt.Tx, string, string) {
	t.Helper()
	tx := bt.NewTx()
	require.NoError(t, tx.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 1000))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
	id := tx.TxID()
	b := &recordBackend{records: map[string]Record{"parent": {Key: []byte("parent"), Data: []byte("spent:" + id), Generation: 7}, id: {Key: []byte(id), Data: []byte("unmined"), Generation: 2}}, txs: map[string]*bt.Tx{id: tx}, seeds: []string{id}}
	tip := Tip{Hash: "2222222222222222222222222222222222222222222222222222222222222222", Height: 200}
	s := &evidenceSource{tip: tip, evidence: map[string]Evidence{id: {TxID: id, RawTx: tx.String(), BlockHash: "3333333333333333333333333333333333333333333333333333333333333333", BlockHeight: 100, Classification: FullySpent}}}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	return b, s, tx, filepath.Join(dir, "manifest.db"), filepath.Join(dir, "journal.db")
}

func TestDiscoveryDefaultNeverWrites(t *testing.T) {
	b, s, _, manifest, _ := recoveryFixture(t)
	summary, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), summary.FullySpent)
	require.Zero(t, b.writes)
}

func TestApplyMarkerFailureNeverDeletesChild(t *testing.T) {
	b, s, tx, manifest, journal := recoveryFixture(t)
	_, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.NoError(t, err)
	b.failMark = true
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
	require.Error(t, err)
	require.Contains(t, b.records, tx.TxID())
	require.Equal(t, "spent:"+tx.TxID(), string(b.records["parent"].Data))
	b.failMark = false
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: s.tip})
	require.NoError(t, err)
	require.NotContains(t, b.records, tx.TxID())
	require.True(t, b.HasMarker(b.records["parent"], tx.TxID()))
}

func TestApplyResumesAmbiguousVerifiedMarker(t *testing.T) {
	b, s, tx, manifest, journal := recoveryFixture(t)
	_, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.NoError(t, err)
	b.failAfterMark = true
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
	require.Error(t, err)
	require.Contains(t, b.records, tx.TxID())
	b.failAfterMark = false
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: s.tip})
	require.NoError(t, err)
	require.NotContains(t, b.records, tx.TxID())
}

func TestApplyRejectsGenerationConflictAndMissingMaintenance(t *testing.T) {
	b, s, tx, manifest, journal := recoveryFixture(t)
	_, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.NoError(t, err)
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Tip: s.tip})
	require.Error(t, err)
	require.Zero(t, b.writes)
	r := b.records[tx.TxID()]
	r.Generation++
	b.records[tx.TxID()] = r
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
	require.Error(t, err)
	require.Contains(t, b.records, tx.TxID())
	require.Zero(t, b.writes)
}

func TestVerifyCompletesStoreRecoveryWhileIdle(t *testing.T) {
	b, s, _, manifest, journal := recoveryFixture(t)
	_, err := discoverForTest(t.Context(), b, s, manifest, nil)
	require.NoError(t, err)
	_, err = Apply(t.Context(), b, s, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: s.tip})
	require.NoError(t, err)
	result, err := Verify(t.Context(), b, s, manifest, journal, allowRecovery)
	require.NoError(t, err)
	require.True(t, result.Complete)
	require.True(t, result.RestartRequired)
}

func (b *recordBackend) Inventory(ctx context.Context, visit func(InventoryRecord) error) error {
	for key, r := range b.records {
		id := key
		master := true
		var page, pages uint32
		if strings.HasSuffix(key, ":page") {
			id = strings.TrimSuffix(key, ":page")
			master = false
			page = 1
		}
		if _, ok := b.records[id+":page"]; ok && master {
			pages = 1
		}
		if key == "parent" {
			id = strings.Repeat("1", 64)
		}
		if !validHash(id) {
			id = strings.Repeat("8", 64)
		}
		candidate := false
		for _, seed := range b.seeds {
			if seed == id {
				candidate = true
			}
		}
		if err := visit(InventoryRecord{Record: r, TxID: id, Master: master, Page: page, ExpectedPages: pages, Candidate: candidate}); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (b *recordBackend) SpendReferences(ctx context.Context, visit func(SpendReference) error) error {
	for key, r := range b.records {
		if strings.HasPrefix(string(r.Data), "spent:") {
			parts := strings.Split(string(r.Data), ":")
			if err := visit(SpendReference{ParentKey: []byte(key), ParentTxID: strings.Repeat("1", 64), ChildTxID: parts[1], Marked: len(parts) == 3}); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}
func (b *recordBackend) SnapshotAbsent(ctx context.Context, tx *bt.Tx) (Snapshot, error) {
	if _, ok := b.records[tx.TxID()]; ok {
		return Snapshot{}, failure("child still exists")
	}
	if _, ok := b.records[tx.TxID()+":page"]; ok {
		return Snapshot{}, failure("child page still exists")
	}
	p, ok := b.records["parent"]
	if !ok {
		return Snapshot{}, failure("missing parent")
	}
	return Snapshot{TxID: tx.TxID(), Parents: []Parent{{Record: p, Child: tx.TxID()}}}, ctx.Err()
}
func discoverForTest(ctx context.Context, b Backend, s Source, path string, progress func(Summary)) (Summary, error) {
	census, ok := b.(CensusBackend)
	if !ok {
		return Summary{}, failure("test backend lacks census")
	}
	if _, err := Inventory(ctx, census, path, allowRecovery); err != nil {
		return Summary{}, err
	}
	return Discover(ctx, b, s, path, allowRecovery, progress)
}
func TestApplyRequiresGuardAndStopsAfterMarker(t *testing.T) {
	for _, mode := range []string{"nil", "before", "after-marker"} {
		t.Run(mode, func(t *testing.T) {
			b, s, tx, path, journal := recoveryFixture(t)
			_, err := discoverForTest(t.Context(), b, s, path, nil)
			require.NoError(t, err)
			var guard Guard
			if mode != "nil" {
				guard = func(context.Context) error {
					if mode == "before" || b.writes > 0 {
						return failure("left IDLE")
					}
					return nil
				}
			}
			_, err = Apply(t.Context(), b, s, path, journal, ApplyOptions{Maintenance: true, Tip: s.tip, Guard: guard})
			require.Error(t, err)
			require.Contains(t, b.records, tx.TxID())
			if mode == "after-marker" {
				require.Equal(t, 1, b.writes)
			} else {
				require.Zero(t, b.writes)
			}
		})
	}
}
func TestAbsentChildRepairOnlyMarksSurvivingParent(t *testing.T) {
	b, s, tx, path, journal := recoveryFixture(t)
	delete(b.records, tx.TxID())
	summary, err := discoverForTest(t.Context(), b, s, path, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, summary.FullySpent)
	_, err = Apply(t.Context(), b, s, path, journal, ApplyOptions{Maintenance: true, Tip: s.tip, Guard: allowRecovery})
	require.NoError(t, err)
	require.Equal(t, 1, b.writes)
	require.True(t, b.HasMarker(b.records["parent"], tx.TxID()))
	_, err = Verify(t.Context(), b, s, path, journal, allowRecovery)
	require.NoError(t, err)
}

func TestApplyDeletesPagesBeforeMasterToKeepResumeOwnership(t *testing.T) {
	b, s, tx, path, journal := recoveryFixture(t)
	pageKey := tx.TxID() + ":page"
	b.records[pageKey] = Record{Key: []byte(pageKey), Data: []byte("page"), Generation: 3}
	_, err := discoverForTest(t.Context(), b, s, path, nil)
	require.NoError(t, err)
	b.failDeleteKey = tx.TxID()
	_, err = Apply(t.Context(), b, s, path, journal, ApplyOptions{Maintenance: true, Tip: s.tip, Guard: allowRecovery})
	require.Error(t, err)
	require.Contains(t, b.records, tx.TxID(), "master must remain until every page is gone")
	require.NotContains(t, b.records, pageKey, "page deletion must precede the attempted master deletion")
	b.failDeleteKey = ""
	_, err = Apply(t.Context(), b, s, path, journal, ApplyOptions{Maintenance: true, Resume: true, Tip: s.tip, Guard: allowRecovery})
	require.NoError(t, err)
	require.NotContains(t, b.records, tx.TxID())
}
