package alert

import (
	"context"
	"strings"

	"github.com/bsv-blockchain/go-alert-system/app/models"
	"github.com/bsv-blockchain/go-alert-system/app/models/model"
	"github.com/mrz1836/go-datastore"

	"github.com/bsv-blockchain/teranode/errors"
)

// syncGenesisKeys makes the store's active public keys match the configured genesis
// keys while the store holds nothing past the genesis alert, and returns the number
// of active keys alerts are verified against.
//
// Alert signatures are checked against the active keys in the store, which
// CreateGenesisAlert seeds only when it first creates the genesis alert. A store
// first started with other genesis keys (for example a testnet node that ran with
// the mainnet keys) keeps them and rejects every alert of its network. Once a later
// alert is stored the active keys may have been rotated by a SetKeys alert, so the
// store is left alone; alerts that fail verification are never stored, so a store
// seeded with the wrong keys never gets past the genesis alert.
func (s *Server) syncGenesisKeys(ctx context.Context) (int, error) {
	opts := model.WithAllDependencies(s.appConfig)

	latest, err := models.GetLatestAlert(ctx, nil, opts)
	if err != nil {
		return 0, errors.NewStorageError("error getting the latest alert", err)
	}

	active, err := models.GetActivePublicKey(ctx, nil, opts)
	if err != nil {
		return 0, errors.NewStorageError("error getting the active public keys", err)
	}

	genesisKeys := uniqueKeys(s.appConfig.GenesisKeys)

	if latest.SequenceNumber != 0 || sameKeys(active, genesisKeys) {
		return len(active), nil
	}

	s.logger.Warnf("[Alert] alert store has %d active keys that differ from the %d configured genesis keys and no alert past the genesis alert, resetting them to the genesis keys", len(active), len(genesisKeys))

	for _, k := range active {
		k.SetOptions(opts) // query results carry no datastore
		k.Active = false
		if err = k.Save(ctx); err != nil {
			return 0, errors.NewStorageError("error deactivating public key %s", k.Key, err)
		}
	}

	for _, key := range genesisKeys {
		k := models.NewPublicKey(opts)
		if err = model.Get(ctx, k, map[string]interface{}{"key": key}, 0, true); err != nil {
			if !errors.Is(err, datastore.ErrNoResults) {
				return 0, errors.NewStorageError("error getting public key %s", key, err)
			}

			k = models.NewPublicKey(opts, model.New())
			k.Key = key
		}

		k.Active = true
		if err = k.Save(ctx); err != nil {
			return 0, errors.NewStorageError("error activating public key %s", key, err)
		}
	}

	return len(genesisKeys), nil
}

// uniqueKeys returns keys without blank or repeated entries, in their original order.
func uniqueKeys(keys []string) []string {
	seen := make(map[string]struct{}, len(keys))
	unique := make([]string, 0, len(keys))

	for _, key := range keys {
		key = strings.TrimSpace(key)
		if _, ok := seen[key]; ok || key == "" {
			continue
		}

		seen[key] = struct{}{}
		unique = append(unique, key)
	}

	return unique
}

// sameKeys reports whether active holds exactly the keys in keys, ignoring order and case.
func sameKeys(active []*models.PublicKey, keys []string) bool {
	activeSet := make(map[string]struct{}, len(active))
	for _, k := range active {
		activeSet[strings.ToLower(k.Key)] = struct{}{}
	}

	if len(activeSet) != len(active) || len(activeSet) != len(keys) {
		return false
	}

	for _, key := range keys {
		if _, ok := activeSet[strings.ToLower(key)]; !ok {
			return false
		}
	}

	return true
}
