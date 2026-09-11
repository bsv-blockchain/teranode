package aerospike

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
)

var _ rr.CensusBackend = (*RecoveryBackend)(nil)
var _ rr.AbsentBackend = (*RecoveryBackend)(nil)

// Inventory visits every native record, including records excluded by normal
// assembly iteration. The queue is bounded; callers persist each result to disk.
func (b *RecoveryBackend) Inventory(ctx context.Context, visit func(rr.InventoryRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	policy := as.NewScanPolicy()
	policy.RecordQueueSize = 128
	policy.MaxConcurrentNodes = max(1, b.ScanConcurrency)
	policy.ReadTouchTTLPercent = -1
	policy.MaxRetries = 0
	records, err := b.client.ScanAll(policy, b.namespace, b.set)
	if err != nil {
		return err
	}
	defer records.Close()
	cache := recoveryPageCache{owners: make(map[string]recoveryPageOwner)}
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
			if result.Record == nil || result.Record.Key == nil {
				return errors.NewProcessingError("scan returned empty record")
			}
			rec := result.Record
			data, err := encodeRecoveryBins(rec.Bins)
			if err != nil {
				return err
			}
			metadata, err := b.client.Operate(recoveryPolicy(ctx, 0), rec.Key, as.ExpReadOp("expiry", as.ExpVoidTime(), as.ExpReadFlagDefault))
			if err != nil {
				return err
			}
			if metadata == nil || metadata.Generation != rec.Generation {
				return errors.NewProcessingError("record changed during census")
			}
			expiry, ok := metadata.Bins["expiry"].(int)
			if !ok || expiry < -1 {
				return errors.NewProcessingError("invalid census expiration")
			}
			item := rr.InventoryRecord{Record: rr.Record{Key: append([]byte(nil), rec.Key.Digest()...), Data: data, Generation: rec.Generation, ExpiresAt: int64(expiry) / int64(time.Second)}}
			if err := b.inventoryIdentity(ctx, rec, &item, &cache); err != nil {
				// Cancellation or read failure is operational, not a schema finding.
				return err
			}
			if err := visit(item); err != nil {
				return err
			}
		}
	}
}

// A bounded scan-local cache avoids regenerating every advertised page key for
// each page. Only output counts are cached, never transaction/script payloads.
type recoveryPageOwner struct {
	bins    as.BinMap
	indexes map[string]uint32
}
type recoveryPageCache struct {
	owners map[string]recoveryPageOwner
	pages  int
}

func (b *RecoveryBackend) inventoryIdentity(ctx context.Context, rec *as.Record, item *rr.InventoryRecord, cache *recoveryPageCache) error {
	fail := func(reason string) { item.Candidate = true; item.Reason = reason }
	raw, ok := rec.Bins[fields.TxID.String()].([]byte)
	if !ok || len(raw) != 32 {
		fail("unknown record ownership: missing or malformed txID")
		return nil
	}
	hash, _ := chainhash.NewHash(raw)
	key, err := b.key(raw)
	if err != nil {
		return err
	}
	masterBins := rec.Bins
	if bytes.Equal(key.Digest(), rec.Key.Digest()) {
		item.Master = true
		item.Record.Key = append([]byte(nil), raw...)
	} else {
		owner, cached := cache.owners[string(raw)]
		if !cached {
			master, err := b.Read(ctx, raw)
			if err != nil {
				return err
			}
			if master == nil {
				fail("unknown page ownership: master is absent")
				return nil
			}
			bins, err := decodeRecoveryBins(master.Data)
			if err != nil {
				return err
			}
			count, err := recoveryInteger(bins, fields.TotalExtraRecs)
			if err != nil || count > 65536 {
				fail("unknown page ownership: unsupported master page count")
				return nil
			}
			owner = recoveryPageOwner{bins: as.BinMap{fields.TotalExtraRecs.String(): bins[fields.TotalExtraRecs.String()], fields.TotalUtxos.String(): bins[fields.TotalUtxos.String()]}, indexes: make(map[string]uint32, count)}
			for page := 1; page <= count; page++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				rawKey := uaerospike.CalculateKeySourceInternal(hash, uint32(page))
				pageKey, err := b.key(rawKey)
				if err != nil {
					return err
				}
				owner.indexes[string(pageKey.Digest())] = uint32(page)
			}
			if cache.pages+count > 131072 || len(cache.owners) >= 1024 {
				cache.owners = make(map[string]recoveryPageOwner)
				cache.pages = 0
			}
			cache.owners[string(raw)] = owner
			cache.pages += count
		}
		masterBins = owner.bins
		page, found := owner.indexes[string(rec.Key.Digest())]
		if !found {
			fail("unknown page ownership: digest does not match advertised pages")
			return nil
		}
		item.Record.Key = uaerospike.CalculateKeySourceInternal(hash, page)
		item.Page = page
	}
	item.TxID = hash.String()
	total, err := recoveryInteger(masterBins, fields.TotalUtxos)
	if err != nil || total < 1 || uint64(total) > math.MaxUint32 {
		fail("invalid total output count")
		return nil
	}
	pages, err := recoveryInteger(masterBins, fields.TotalExtraRecs)
	if err != nil || pages != (total-1)/b.batchSize || pages > 65536 {
		fail("invalid or unsupported page inventory")
		return nil
	}
	if item.Master {
		item.ExpectedPages = uint32(pages) // #nosec G115 -- page count is nonnegative and bounded by 65536 above.
	}
	if err := recoveryIdentity(rec.Bins, hash); err != nil {
		fail(err.Error())
	}
	if _, err := recoveryOutputs(rec.Bins, min(b.batchSize, total-int(item.Page)*b.batchSize)); err != nil {
		fail(err.Error())
	}
	if !item.Master {
		return nil
	}
	_, unmined := rec.Bins[fields.UnminedSince.String()]
	if unmined {
		item.Candidate = true
	}
	ids, ok := rec.Bins[fields.BlockIDs.String()].([]interface{})
	if !ok || len(ids) == 0 {
		item.Candidate = true
	}
	for _, field := range []fields.FieldName{fields.BlockIDs, fields.BlockHeights, fields.SubtreeIdxs} {
		list, valid := rec.Bins[field.String()].([]interface{})
		if !valid || len(list) != len(ids) {
			fail("missing or inconsistent mined metadata")
			continue
		}
		for _, value := range list {
			if n, valid := value.(int); !valid || n < 0 {
				fail("malformed mined metadata")
			}
		}
	}
	return nil
}

// SpendReferences includes mined parents and their pages, even when unsafe flags
// prevent mutation. Unsupported records are already findings in Inventory.
func (b *RecoveryBackend) SpendReferences(ctx context.Context, visit func(rr.SpendReference) error) error {
	return b.Inventory(ctx, func(item rr.InventoryRecord) error {
		if item.TxID == "" {
			return nil
		}
		bins, err := decodeRecoveryBins(item.Record.Data)
		if err != nil {
			return err
		}
		values, ok := bins[fields.Utxos.String()].([]interface{})
		if !ok {
			return nil
		}
		markers, err := recoveryMarkers(bins)
		if err != nil {
			// Inventory reports the malformed marker map. Keep decodable
			// spend edges so unknown components cannot disappear from the graph.
			markers = nil
		}
		masterMarkers := markers
		if !item.Master {
			hash, err := chainhash.NewHashFromStr(item.TxID)
			if err != nil {
				return err
			}
			_, masterBins, err := b.readRequired(ctx, hash[:])
			if err != nil {
				return err
			}
			masterMarkers, err = recoveryMarkers(masterBins)
			if err != nil {
				masterMarkers = nil
			}
		}
		for i, value := range values {
			raw, ok := value.([]byte)
			if !ok || len(raw) != 68 {
				continue
			}
			child, _ := chainhash.NewHash(raw[32:64])
			offset := uint64(item.Page)*uint64(b.batchSize) + uint64(i) // #nosec G115 -- constructor requires positive uint32 batch size; range index is nonnegative.
			if offset > math.MaxUint32 {
				return errors.NewProcessingError("spend reference output overflow")
			}
			ref := rr.SpendReference{ParentKey: append([]byte(nil), item.Record.Key...), ParentTxID: item.TxID, Vout: uint32(offset), ChildTxID: child.String(), Vin: binary.LittleEndian.Uint32(raw[64:]), Marked: markers[child.String()] == true && masterMarkers[child.String()] == true}
			if err := visit(ref); err != nil {
				return err
			}
		}
		return nil
	})
}
