package redis

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *Store) SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error {
	return s.SetMinedMulti(ctx, []*chainhash.Hash{hash}, blockID)
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	setMinedFn := fmt.Sprintf("setMined_%s", s.version)

	for i, hash := range hashes {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errors.NewStorageError("timeout setMined %d of %d txns", i, len(hashes))
			}

			return errors.NewStorageError("context cancelled setMined %d of %d txns", i, len(hashes))

		default:
			cmd := s.client.Do(ctx,
				"FCALL",
				setMinedFn,             // function name§
				1,                      // number of key args
				hash.String(),          // key[1]
				blockID,                // args[1] - offset
				s.expiration.Seconds(), // args[2] - ttl
			)

			if err := cmd.Err(); err != nil {
				return err
			}

			text, err := cmd.Text()
			if err != nil {
				return err
			}

			res, err := parseLuaReturnValue(text)
			if err != nil {
				return errors.NewProcessingError("Could not parse redis LUA response", err)
			}

			if res.returnValue != LuaOk {
				return errors.NewProcessingError("Redis setMined: %s", res)
			}
		}
	}

	return nil
}
