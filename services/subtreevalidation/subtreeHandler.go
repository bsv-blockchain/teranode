package subtreevalidation

import (
	"context"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (u *SubtreeValidation) subtreeHandler(msg util.KafkaMessage) {
	if msg.Message != nil {
		if len(msg.Message.Value) < 32 {
			u.logger.Errorf("Received subtree message of %d bytes", len(msg.Message.Value))
			return
		}

		hash, err := chainhash.NewHash(msg.Message.Value[:32])
		if err != nil {
			u.logger.Errorf("Failed to parse subtree hash from message: %v", err)
			return
		}

		var baseUrl string
		if len(msg.Message.Value) > 32 {
			baseUrl = string(msg.Message.Value[32:])
		}

		u.logger.Infof("Received subtree message for %s from %s", hash.String(), baseUrl)

		gotLock, err := u.tryLockIfNotExists(context.Background(), hash)
		if err != nil {
			u.logger.Infof("error getting lock for Subtree %s", hash.String())
			return
		}

		if !gotLock {
			u.logger.Infof("Subtree %s already exists", hash.String())
			return
		}

		v := ValidateSubtree{
			SubtreeHash:   *hash,
			BaseUrl:       baseUrl,
			SubtreeHashes: nil,
			AllowFailFast: false,
		}

		// Call the validateSubtreeInternal method
		if err := u.validateSubtreeInternal(context.Background(), v); err != nil {
			u.logger.Errorf("Failed to validate subtree %s: %v", hash.String(), err)
		}
	}

}
