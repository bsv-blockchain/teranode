package replayrecovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
)

type localTransactionReader interface {
	Transaction(context.Context, string) (*bt.Tx, error)
}

type parentVerifier interface {
	VerifyParent(context.Context, Parent) error
}

func verifyStore(ctx context.Context, m *manifest, j *journal, backend Backend, source Source, tip Tip, guard Guard) error {
	return m.each(ctx, true, func(e Entry) error {
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		for _, r := range e.Snapshot.Records {
			current, err := backend.Read(ctx, r.Key)
			if err != nil {
				return err
			}
			if current != nil {
				return failure("repaired transaction record reappeared: %s", e.Evidence.TxID)
			}
		}
		for _, p := range e.Snapshot.Parents {
			current, err := backend.Read(ctx, p.Record.Key)
			if err != nil {
				return err
			}
			if current == nil {
				step, err := j.step(fmt.Sprintf("delete/%x", p.Record.Key))
				if err != nil {
					return err
				}
				if step == nil || !step.Done {
					return failure("required surviving parent is missing: %s", e.Evidence.TxID)
				}
			} else {
				if !backend.HasMarker(*current, p.Child) {
					return failure("required replay marker disappeared: %s", p.Child)
				}
				verifier, ok := backend.(parentVerifier)
				if !ok {
					return failure("backend cannot verify preserved parent spends: %w", ErrIncomplete)
				}
				if err := verifier.VerifyParent(ctx, p); err != nil {
					return err
				}
			}
		}
		ev, err := source.Check(ctx, e.Evidence.TxID, tip)
		if err != nil {
			return err
		}
		if ev.TxID != e.Evidence.TxID || ev.RawTx != e.Evidence.RawTx || ev.Tip != tip || ev.Classification != FullySpent {
			return failure("repaired transaction no longer proven fully spent: %s", e.Evidence.TxID)
		}
		return nil
	})
}

// Verify checks persisted outcomes while IDLE; restarting assembly is an operator step.
func Verify(ctx context.Context, backend Backend, source Source, manifestPath, journalPath string, guard Guard) (result Summary, err error) {
	if err = checkGuard(ctx, guard); err != nil {
		return result, err
	}
	m, err := openManifest(manifestPath)
	if err != nil {
		return result, err
	}
	defer func() { err = combineErrors(err, m.Close()) }()
	if backend.Identity() != m.header.Identity {
		return result, failure("manifest belongs to another backend")
	}
	j, err := openJournal(journalPath, true, m.digest, backend.Identity())
	if err != nil {
		return result, err
	}
	defer func() { err = combineErrors(err, j.Close()) }()
	applied, err := j.get("applied")
	if err != nil {
		return result, err
	}
	if applied == nil {
		return result, failure("apply has not completed")
	}
	var tip Tip
	if err = json.Unmarshal(applied, &tip); err != nil {
		return result, err
	}
	if tip != m.header.Tip {
		return result, failure("journal tip differs from sealed plan")
	}
	if err = requireTip(ctx, source, tip); err != nil {
		return result, err
	}
	result, err = m.summary()
	if err != nil {
		return result, err
	}
	result.Stage = "verifying"
	result.Complete = false
	result.Applied = true
	var intents int
	if err = j.db.QueryRow("SELECT COUNT(*) FROM state WHERE key LIKE 'step/%'").Scan(&intents); err != nil {
		return result, err
	}
	result.RestartRequired = intents > 0
	if err = inventoryMatches(ctx, m, j, backend, guard); err != nil {
		return result, err
	}
	if err = verifyStore(ctx, m, j, backend, source, tip, guard); err != nil {
		return result, err
	}
	if err = j.db.QueryRow("SELECT COUNT(*) FROM work WHERE done=1").Scan(&result.Repaired); err != nil {
		return result, err
	}
	if err = checkGuard(ctx, guard); err != nil {
		return result, err
	}
	if err = requireTip(ctx, source, tip); err != nil {
		return result, err
	}
	result.Complete = m.header.GraphComplete && result.Unknown == 0 && result.Live == 0 && result.Blocked == 0 && result.Findings == 0
	result.Stage = "verified"
	report, _ := json.Marshal(result)
	if err = j.put("verified", report); err != nil {
		return result, err
	}
	if !result.Complete {
		return result, ErrIncomplete
	}
	return result, nil
}
