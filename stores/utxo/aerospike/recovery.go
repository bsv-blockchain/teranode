package aerospike

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

// RecoveryBackend uses only direct, bounded client calls: no store startup,
// cleaners, indexes, wrapper retries, or external transaction deletion.
type RecoveryBackend struct {
	// ScanConcurrency limits cluster scan fan-out; zero uses one node at a time.
	ScanConcurrency int
	client          *as.Client
	namespace, set  string
	batchSize       int
	target          string
}

var _ rr.Backend = (*RecoveryBackend)(nil)

func NewRecoveryBackend(client *as.Client, namespace, set string, batchSize int, configuredTarget ...string) (*RecoveryBackend, error) {
	if client == nil || namespace == "" || set == "" || batchSize <= 0 || uint64(batchSize) > math.MaxUint32 {
		return nil, errors.NewProcessingError("invalid recovery backend configuration")
	}
	if len(configuredTarget) > 1 {
		return nil, errors.NewProcessingError("only one recovery target identity is supported")
	}
	target := ""
	if len(configuredTarget) == 1 {
		target = configuredTarget[0]
	} else {
		hosts := make([]string, 0)
		for _, node := range client.GetNodes() {
			hosts = append(hosts, node.GetHost().String())
		}
		sort.Strings(hosts)
		target = strings.Join(hosts, ",")
	}
	if target == "" || strings.ContainsAny(target, "@/?#\r\n") {
		return nil, errors.NewProcessingError("recovery target must be a credential-free host identity")
	}
	return &RecoveryBackend{client: client, namespace: namespace, set: set, batchSize: batchSize, target: target}, nil
}
func (b *RecoveryBackend) Identity() string {
	return fmt.Sprintf("aerospike:%s:%s:%s:outputs-per-record=%d", b.target, b.namespace, b.set, b.batchSize)
}
func recoveryPolicy(ctx context.Context, generation uint32) *as.WritePolicy {
	p := as.NewWritePolicy(generation, as.TTLDontUpdate)
	p.MaxRetries = 0
	p.ReadTouchTTLPercent = -1
	p.CommitLevel = as.COMMIT_ALL
	p.TotalTimeout = 10 * time.Second
	p.SocketTimeout = 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < p.TotalTimeout {
			p.TotalTimeout = remaining
			p.SocketTimeout = remaining
		}
	}
	return p
}
func (b *RecoveryBackend) key(raw []byte) (*as.Key, error) {
	if len(raw) != 32 && len(raw) != 36 {
		return nil, errors.NewProcessingError("invalid recovery key length %d", len(raw))
	}
	return as.NewKey(b.namespace, b.set, raw)
}

// Read snapshots raw bins then reads absolute server expiration. Both reads must
// have the same generation; read touch is disabled. READ_ALL cannot be combined
// with expression reads in Aerospike, hence the separate metadata operation.
func (b *RecoveryBackend) Read(ctx context.Context, key []byte) (*rr.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	k, err := b.key(key)
	if err != nil {
		return nil, err
	}
	p := recoveryPolicy(ctx, 0)
	rec, aerr := b.client.Get(&p.BasePolicy, k)
	if aerr != nil {
		if aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil, nil
		}
		return nil, aerr
	}
	if rec == nil {
		return nil, errors.NewProcessingError("missing recovery readback")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	metadata, aerr := b.client.Operate(p, k, as.ExpReadOp("expiry", as.ExpVoidTime(), as.ExpReadFlagDefault))
	if aerr != nil {
		return nil, aerr
	}
	if metadata == nil || metadata.Generation != rec.Generation {
		return nil, errors.NewProcessingError("record changed while reading expiry")
	}
	// ExpVoidTime returns -1 for records without expiration. Division maps it to 0.
	expiry, ok := metadata.Bins["expiry"].(int)
	if !ok || expiry < -1 {
		return nil, errors.NewProcessingError("invalid absolute expiry: %T %v", metadata.Bins["expiry"], metadata.Bins["expiry"])
	}
	data, err := encodeRecoveryBins(rec.Bins)
	if err != nil {
		return nil, err
	}
	return &rr.Record{Key: append([]byte(nil), key...), Data: data, Generation: rec.Generation, ExpiresAt: int64(expiry) / int64(time.Second)}, nil
}
func (b *RecoveryBackend) Equal(a, c rr.Record) bool {
	return bytes.Equal(a.Key, c.Key) && bytes.Equal(a.Data, c.Data) && a.Generation == c.Generation && a.ExpiresAt == c.ExpiresAt
}
func recoveryMarkers(bins as.BinMap) (map[interface{}]interface{}, error) {
	value, exists := bins[fields.DeletedChildren.String()]
	if !exists {
		return make(map[interface{}]interface{}), nil
	}
	m, ok := value.(map[interface{}]interface{})
	if !ok {
		return nil, errors.NewProcessingError("malformed deletedChildren")
	}
	for k, v := range m {
		s, ok := k.(string)
		if !ok || len(s) != 64 || v != true {
			return nil, errors.NewProcessingError("malformed deletedChildren entry")
		}
		if _, err := chainhash.NewHashFromStr(s); err != nil {
			return nil, err
		}
	}
	return m, nil
}
func (b *RecoveryBackend) HasMarker(rec rr.Record, child string) bool {
	bins, err := decodeRecoveryBins(rec.Data)
	if err != nil {
		return false
	}
	m, err := recoveryMarkers(bins)
	return err == nil && m[child] == true
}
func (b *RecoveryBackend) MatchesMarked(before, after rr.Record, child string) bool {
	// Aerospike AP generations wrap after 65535. Refuse wrap here: a fresh audit
	// is safer than claiming ownership of an ambiguous wrapped generation.
	if before.Generation >= 65535 || after.Generation != before.Generation+1 || before.ExpiresAt != after.ExpiresAt || !bytes.Equal(before.Key, after.Key) {
		return false
	}
	bins, err := decodeRecoveryBins(before.Data)
	if err != nil {
		return false
	}
	m, err := recoveryMarkers(bins)
	if err != nil || m[child] == true {
		return false
	}
	m[child] = true
	bins[fields.DeletedChildren.String()] = m
	data, err := encodeRecoveryBins(bins)
	return err == nil && bytes.Equal(data, after.Data)
}
func (b *RecoveryBackend) Mark(ctx context.Context, before rr.Record, child string) (rr.Record, error) {
	if len(child) != 64 {
		return rr.Record{}, errors.NewProcessingError("invalid marker transaction ID")
	}
	if _, err := chainhash.NewHashFromStr(child); err != nil {
		return rr.Record{}, err
	}
	current, err := b.Read(ctx, before.Key)
	if err != nil {
		return rr.Record{}, err
	}
	if current == nil || !b.Equal(before, *current) {
		return rr.Record{}, errors.NewProcessingError("parent changed or absent before marker")
	}
	bins, err := decodeRecoveryBins(before.Data)
	if err != nil {
		return rr.Record{}, err
	}
	markers, err := recoveryMarkers(bins)
	if err != nil {
		return rr.Record{}, err
	}
	if markers[child] == true {
		return before, nil
	}
	if before.Generation >= 65535 {
		return rr.Record{}, errors.NewProcessingError("generation wrap requires fresh audit")
	}
	if err = ctx.Err(); err != nil {
		return rr.Record{}, err
	}
	k, err := b.key(before.Key)
	if err != nil {
		return rr.Record{}, err
	}
	p := recoveryPolicy(ctx, before.Generation)
	p.RecordExistsAction = as.UPDATE_ONLY
	p.GenerationPolicy = as.EXPECT_GEN_EQUAL
	markers[child] = true
	if err := b.client.Put(p, k, as.BinMap{fields.DeletedChildren.String(): markers}); err != nil {
		return rr.Record{}, err
	}
	after, err := b.Read(ctx, before.Key)
	if err != nil {
		return rr.Record{}, err
	}
	if after == nil || !b.MatchesMarked(before, *after, child) {
		return rr.Record{}, errors.NewProcessingError("marker readback mismatch")
	}
	return *after, nil
}
func (b *RecoveryBackend) Delete(ctx context.Context, before rr.Record) error {
	current, err := b.Read(ctx, before.Key)
	if err != nil {
		return err
	}
	if current == nil || !b.Equal(before, *current) {
		return errors.NewProcessingError("record changed or absent before delete")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	key, err := b.key(before.Key)
	if err != nil {
		return err
	}
	p := recoveryPolicy(ctx, before.Generation)
	p.GenerationPolicy = as.EXPECT_GEN_EQUAL
	existed, aerr := b.client.Delete(p, key)
	if aerr != nil {
		return aerr
	}
	if !existed {
		return errors.NewProcessingError("record disappeared during delete")
	}
	after, err := b.Read(ctx, before.Key)
	if err != nil {
		return err
	}
	if after != nil {
		return errors.NewProcessingError("deleted record reappeared")
	}
	return nil
}

// Scan streams every partition, selecting records by metadata rather than an
// index that recovery would need to create. Queue and projected record size are bounded.
func (b *RecoveryBackend) Scan(ctx context.Context, visit func(string) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := as.NewScanPolicy()
	p.RecordQueueSize = 128
	p.ReadTouchTTLPercent = -1
	p.MaxRetries = 0
	records, err := b.client.ScanAll(p, b.namespace, b.set, fields.TxID.String(), fields.UnminedSince.String(), fields.BlockIDs.String(), fields.TotalExtraRecs.String())
	if err != nil {
		return err
	}
	defer records.Close()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-records.Results():
			if !ok {
				return nil
			}
			if result.Err != nil {
				return result.Err
			}
			if result.Record == nil {
				return errors.NewProcessingError("scan returned empty record")
			}
			bins := result.Record.Bins
			_, unmined := bins[fields.UnminedSince.String()]
			ids, hasIDs := bins[fields.BlockIDs.String()]
			_, master := bins[fields.TotalExtraRecs.String()]
			hash, hasHash := bins[fields.TxID.String()].([]byte)
			if !hasHash || len(hash) != 32 {
				if !unmined && !master && !hasIDs {
					continue
				}
				return errors.NewProcessingError("malformed unmined txID")
			}
			expected, err := b.key(hash)
			if err != nil {
				return err
			}
			if !bytes.Equal(expected.Digest(), result.Record.Key.Digest()) {
				if unmined || master || hasIDs {
					return errors.NewProcessingError("master metadata on non-master record")
				}
				continue
			}
			if !unmined {
				list, ok := ids.([]interface{})
				if hasIDs && ok && len(list) > 0 {
					continue
				}
			}
			h, err := chainhash.NewHash(hash)
			if err != nil {
				return err
			}
			if err := visit(h.String()); err != nil {
				return err
			}
		}
	}
}
func recoveryInteger(bins as.BinMap, field fields.FieldName) (int, error) {
	v, ok := bins[field.String()].(int)
	if !ok || v < 0 {
		return 0, errors.NewProcessingError("missing or malformed %s", field)
	}
	return v, nil
}
func recoveryIdentity(bins as.BinMap, hash *chainhash.Hash) error {
	id, ok := bins[fields.TxID.String()].([]byte)
	if !ok || !bytes.Equal(id, hash[:]) {
		return errors.NewProcessingError("record transaction identity mismatch")
	}
	for _, field := range []fields.FieldName{fields.Locked, fields.Conflicting, fields.Creating} {
		if value, exists := bins[field.String()]; exists {
			flag, ok := value.(bool)
			if !ok || flag {
				return errors.NewProcessingError("unsafe %s state", field)
			}
		}
	}
	_, err := recoveryMarkers(bins)
	return err
}
func (b *RecoveryBackend) readRequired(ctx context.Context, key []byte) (rr.Record, as.BinMap, error) {
	r, err := b.Read(ctx, key)
	if err != nil {
		return rr.Record{}, nil, err
	}
	if r == nil {
		return rr.Record{}, nil, errors.NewProcessingError("required recovery record missing: %x", key)
	}
	bins, err := decodeRecoveryBins(r.Data)
	return *r, bins, err
}
func recoveryInputs(bins as.BinMap) ([]*bt.Input, error) {
	raw, ok := bins[fields.Inputs.String()].([]interface{})
	if !ok {
		return nil, errors.NewProcessingError("raw inputs unavailable (external records require archive evidence)")
	}
	inputs := make([]*bt.Input, len(raw))
	for i, value := range raw {
		data, ok := value.([]byte)
		if !ok {
			return nil, errors.NewProcessingError("malformed raw input")
		}
		input := new(bt.Input)
		reader := bytes.NewReader(data)
		if _, err := input.ReadFromExtended(reader); err != nil {
			return nil, err
		}
		if reader.Len() != 0 {
			return nil, errors.NewProcessingError("trailing raw input bytes")
		}
		inputs[i] = input
	}
	return inputs, nil
}
func (b *RecoveryBackend) Inputs(ctx context.Context, txid string) ([]string, error) {
	if len(txid) != 64 {
		return nil, errors.NewProcessingError("invalid txid")
	}
	h, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		return nil, err
	}
	_, bins, err := b.readRequired(ctx, h[:])
	if err != nil {
		return nil, err
	}
	if err := recoveryIdentity(bins, h); err != nil {
		return nil, err
	}
	inputs, err := recoveryInputs(bins)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(inputs))
	for _, in := range inputs {
		if in.PreviousTxIDChainHash() == nil {
			return nil, errors.NewProcessingError("input hash missing")
		}
		result = append(result, in.PreviousTxIDChainHash().String())
	}
	return result, nil
}

func (b *RecoveryBackend) Snapshot(ctx context.Context, tx *bt.Tx) (rr.Snapshot, error) {
	result := rr.Snapshot{}
	if tx == nil || len(tx.Outputs) == 0 || len(tx.Inputs) == 0 || tx.IsCoinbase() {
		return result, errors.NewProcessingError("recovery requires non-coinbase transaction with inputs and outputs")
	}
	hash := tx.TxIDChainHash()
	result.TxID = hash.String()
	master, bins, err := b.readRequired(ctx, hash[:])
	if err != nil {
		return result, err
	}
	if err := recoveryIdentity(bins, hash); err != nil {
		return result, err
	}
	for _, field := range []fields.FieldName{fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs} {
		list, ok := bins[field.String()].([]interface{})
		if !ok || len(list) != 0 {
			return result, errors.NewProcessingError("child %s must be empty", field)
		}
	}
	if _, err := recoveryInteger(bins, fields.UnminedSince); err != nil {
		return result, err
	}
	total, err := recoveryInteger(bins, fields.TotalUtxos)
	if err != nil || total != len(tx.Outputs) {
		return result, errors.NewProcessingError("child output count mismatch")
	}
	pages, err := recoveryInteger(bins, fields.TotalExtraRecs)
	if err != nil || pages != (total-1)/b.batchSize {
		return result, errors.NewProcessingError("child page inventory mismatch")
	}
	version, err := recoveryInteger(bins, fields.Version)
	if err != nil || int64(version) != int64(tx.Version) {
		return result, errors.NewProcessingError("child version mismatch")
	}
	lock, err := recoveryInteger(bins, fields.LockTime)
	if err != nil || int64(lock) != int64(tx.LockTime) {
		return result, errors.NewProcessingError("child locktime mismatch")
	}
	external, hasExternal := bins[fields.External.String()]
	if hasExternal && external != true {
		return result, errors.NewProcessingError("malformed external flag")
	}
	if external != true {
		inputs, err := recoveryInputs(bins)
		if err != nil {
			return result, err
		}
		if len(inputs) != len(tx.Inputs) {
			return result, errors.NewProcessingError("child input count mismatch")
		}
		for i, in := range inputs {
			if !bytes.Equal(in.Bytes(false), tx.Inputs[i].Bytes(false)) {
				return result, errors.NewProcessingError("child input identity mismatch")
			}
		}
		outputs, ok := bins[fields.Outputs.String()].([]interface{})
		if !ok || len(outputs) != total {
			return result, errors.NewProcessingError("child raw outputs mismatch")
		}
		for i, v := range outputs {
			raw, ok := v.([]byte)
			if !ok || !bytes.Equal(raw, tx.Outputs[i].Bytes()) {
				return result, errors.NewProcessingError("child raw output mismatch")
			}
		}
	}
	hashes, err := utxo.GetUtxoHashes(tx, hash)
	if err != nil {
		return result, err
	}
	dependencies := make(map[string]bool)
	for page := 0; page <= pages; page++ {
		record, pageBins := master, bins
		if page > 0 {
			record, pageBins, err = b.readRequired(ctx, uaerospike.CalculateKeySourceInternal(hash, uint32(page)))
			if err != nil {
				return result, err
			}
		}
		if err := recoveryIdentity(pageBins, hash); err != nil {
			return result, err
		}
		coinbase, ok := pageBins[fields.IsCoinbase.String()].(bool)
		if !ok || coinbase {
			return result, errors.NewProcessingError("child coinbase metadata mismatch")
		}
		size, err := recoveryInteger(pageBins, fields.SizeInBytes)
		if err != nil || size != tx.Size() {
			return result, errors.NewProcessingError("child size metadata mismatch")
		}
		pageVersion, err := recoveryInteger(pageBins, fields.Version)
		if err != nil || pageVersion != version {
			return result, errors.NewProcessingError("child page version mismatch")
		}
		pageLock, err := recoveryInteger(pageBins, fields.LockTime)
		if err != nil || pageLock != lock {
			return result, errors.NewProcessingError("child page locktime mismatch")
		}
		want := min(b.batchSize, total-page*b.batchSize)
		outputs, err := recoveryOutputs(pageBins, want)
		if err != nil {
			return result, err
		}
		for i, raw := range outputs {
			if raw == nil {
				continue
			}
			if !bytes.Equal(raw[:32], hashes[page*b.batchSize+i][:]) {
				return result, errors.NewProcessingError("child UTXO hash mismatch")
			}
			if len(raw) == 68 {
				spender, _ := chainhash.NewHash(raw[32:64])
				if spender.String() == result.TxID {
					return result, errors.NewProcessingError("self-spending child")
				}
				dependencies[spender.String()] = true
			}
		}
		result.Records = append(result.Records, record)
	}
	if err := b.snapshotParents(ctx, tx, &result); err != nil {
		return result, err
	}

	for id := range dependencies {
		result.Dependencies = append(result.Dependencies, id)
	}
	sort.Strings(result.Dependencies)
	return result, nil
}
func recoveryOutputs(bins as.BinMap, want int) ([][]byte, error) {
	list, ok := bins[fields.Utxos.String()].([]interface{})
	if !ok || len(list) != want {
		return nil, errors.NewProcessingError("UTXO page length mismatch")
	}
	result := make([][]byte, len(list))
	count, spent := 0, 0
	for i, v := range list {
		if v == nil {
			continue
		}
		raw, ok := v.([]byte)
		if !ok || (len(raw) != 32 && len(raw) != 68) {
			return nil, errors.NewProcessingError("malformed UTXO spend")
		}
		count++
		if len(raw) == 68 {
			spent++
		}
		result[i] = raw
	}
	n, err := recoveryInteger(bins, fields.RecordUtxos)
	if err != nil || n != count {
		return nil, errors.NewProcessingError("UTXO count mismatch")
	}
	n, err = recoveryInteger(bins, fields.SpentUtxos)
	if err != nil || n != spent {
		return nil, errors.NewProcessingError("spent UTXO count mismatch")
	}
	return result, nil
}

// Transaction reconstructs and rehashes inline records for independent candidate
// auditing. External-only records require an archive provider and fail closed.
func (b *RecoveryBackend) Transaction(ctx context.Context, txid string) (*bt.Tx, error) {
	if len(txid) != 64 {
		return nil, errors.NewProcessingError("invalid txid")
	}
	hash, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		return nil, err
	}
	_, bins, err := b.readRequired(ctx, hash[:])
	if err != nil {
		return nil, err
	}
	if err := recoveryIdentity(bins, hash); err != nil {
		return nil, err
	}
	inputs, err := recoveryInputs(bins)
	if err != nil {
		return nil, err
	}
	version, err := recoveryInteger(bins, fields.Version)
	if err != nil || int64(version) > math.MaxUint32 {
		return nil, errors.NewProcessingError("invalid transaction version")
	}
	lock, err := recoveryInteger(bins, fields.LockTime)
	if err != nil || int64(lock) > math.MaxUint32 {
		return nil, errors.NewProcessingError("invalid transaction locktime")
	}
	values, ok := bins[fields.Outputs.String()].([]interface{})
	if !ok || len(values) == 0 {
		return nil, errors.NewProcessingError("raw outputs unavailable")
	}
	outputs := make([]*bt.Output, len(values))
	for i, value := range values {
		raw, ok := value.([]byte)
		if !ok {
			return nil, errors.NewProcessingError("malformed raw output")
		}
		output := new(bt.Output)
		reader := bytes.NewReader(raw)
		if _, err := output.ReadFrom(reader); err != nil {
			return nil, err
		}
		if reader.Len() != 0 {
			return nil, errors.NewProcessingError("trailing raw output bytes")
		}
		outputs[i] = output
	}
	version32, err := safeconversion.IntToUint32(version)
	if err != nil {
		return nil, err
	}
	lock32, err := safeconversion.IntToUint32(lock)
	if err != nil {
		return nil, err
	}
	tx := &bt.Tx{Inputs: inputs, Outputs: outputs, Version: version32, LockTime: lock32}
	if !tx.TxIDChainHash().IsEqual(hash) {
		return nil, errors.NewProcessingError("raw transaction identity mismatch")
	}
	return tx, nil
}

// VerifyParent permits normal updates to unrelated outputs after restart while
// preserving every original input spend protected by this repair's marker.
func (b *RecoveryBackend) VerifyParent(ctx context.Context, parent rr.Parent) error {
	if len(parent.Child) != 64 {
		return errors.NewProcessingError("invalid protected child identity")
	}
	child, err := chainhash.NewHashFromStr(parent.Child)
	if err != nil {
		return err
	}
	if len(parent.Record.Key) != 32 && len(parent.Record.Key) != 36 {
		return errors.NewProcessingError("invalid protected parent key")
	}
	hash, err := chainhash.NewHash(parent.Record.Key[:32])
	if err != nil {
		return err
	}
	before, err := decodeRecoveryBins(parent.Record.Data)
	if err != nil {
		return err
	}
	current, bins, err := b.readRequired(ctx, parent.Record.Key)
	if err != nil {
		return err
	}
	if err := recoveryIdentity(before, hash); err != nil {
		return err
	}
	if err := recoveryIdentity(bins, hash); err != nil {
		return err
	}
	if !b.HasMarker(current, parent.Child) {
		return errors.NewProcessingError("required parent replay marker missing")
	}
	list, ok := before[fields.Utxos.String()].([]interface{})
	if !ok {
		return errors.NewProcessingError("protected parent UTXOs malformed")
	}
	original, err := recoveryOutputs(before, len(list))
	if err != nil {
		return err
	}
	outputs, err := recoveryOutputs(bins, len(list))
	if err != nil {
		return err
	}
	for i, spent := range original {
		if len(spent) == 68 && bytes.Equal(spent[32:64], child[:]) && !bytes.Equal(spent, outputs[i]) {
			return errors.NewProcessingError("protected original input spend changed")
		}
	}
	return nil
}

// SnapshotAbsent preserves surviving input owners without treating missing records
// as deletions performed by this recovery job. Global census handles unknown pages.
func (b *RecoveryBackend) SnapshotAbsent(ctx context.Context, tx *bt.Tx) (rr.Snapshot, error) {
	result := rr.Snapshot{}
	if tx == nil || len(tx.Outputs) == 0 || len(tx.Inputs) == 0 || tx.IsCoinbase() {
		return result, errors.NewProcessingError("recovery requires non-coinbase transaction with inputs and outputs")
	}
	result.TxID = tx.TxID()
	for page := 0; page <= (len(tx.Outputs)-1)/b.batchSize; page++ {
		record, err := b.Read(ctx, uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), uint32(page)))
		if err != nil {
			return result, err
		}
		if record != nil {
			return result, errors.NewProcessingError("absent child record or page is present")
		}
	}
	if err := b.snapshotParents(ctx, tx, &result); err != nil {
		return result, err
	}
	for _, parent := range result.Parents {
		if parent.Record.ExpiresAt != 0 {
			return result, errors.NewProcessingError("finite parent expiration cannot be safely guarded")
		}
	}
	return result, nil
}

func (b *RecoveryBackend) snapshotParents(ctx context.Context, tx *bt.Tx, result *rr.Snapshot) error {
	hash := tx.TxIDChainHash()
	seen := make(map[string]bool)
	for vin, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()
		if parentHash == nil || parentHash.IsEqual(hash) {
			return errors.NewProcessingError("invalid parent identity")
		}
		parent, parentBins, err := b.readRequired(ctx, parentHash[:])
		if err != nil {
			return err
		}
		if err := recoveryIdentity(parentBins, parentHash); err != nil {
			return err
		}
		parentTotal, err := recoveryInteger(parentBins, fields.TotalUtxos)
		if err != nil || int64(input.PreviousTxOutIndex) >= int64(parentTotal) {
			return errors.NewProcessingError("parent output index mismatch")
		}
		parentPages, err := recoveryInteger(parentBins, fields.TotalExtraRecs)
		if err != nil || parentPages != (parentTotal-1)/b.batchSize {
			return errors.NewProcessingError("parent page inventory mismatch")
		}
		addressed, outputBins := parent, parentBins
		page := int(input.PreviousTxOutIndex) / b.batchSize
		if page > 0 {
			addressed, outputBins, err = b.readRequired(ctx, uaerospike.CalculateKeySource(parentHash, input.PreviousTxOutIndex, b.batchSize))
			if err != nil {
				return err
			}
			if err := recoveryIdentity(outputBins, parentHash); err != nil {
				return err
			}
		}
		outputs, err := recoveryOutputs(outputBins, min(b.batchSize, parentTotal-page*b.batchSize))
		if err != nil {
			return err
		}
		raw := outputs[int(input.PreviousTxOutIndex)%b.batchSize]
		if len(raw) != 68 || !bytes.Equal(raw[32:64], hash[:]) || uint64(binary.LittleEndian.Uint32(raw[64:])) != uint64(vin) {
			return errors.NewProcessingError("parent output does not retain original input spend")
		}
		for _, record := range []rr.Record{parent, addressed} {
			if !seen[string(record.Key)] {
				seen[string(record.Key)] = true
				result.Parents = append(result.Parents, rr.Parent{Record: record, Child: result.TxID})
			}
		}
	}
	return nil
}
