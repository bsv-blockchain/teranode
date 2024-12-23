// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"bytes"
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/libsv/go-bt/v2/chainhash"
)

// txmetaHandler processes Kafka messages containing transaction metadata.
// It handles both addition and deletion of transaction metadata in the cache.
func (u *Server) txmetaHandler(msg *kafka.KafkaMessage) error {
	if msg != nil && len(msg.Value) > chainhash.HashSize {
		startTime := time.Now()

		hash := chainhash.Hash(msg.Value[:chainhash.HashSize])

		// check whether the bytes == delete
		if bytes.Equal(msg.Value[chainhash.HashSize:], []byte("delete")) {
			if err := u.DelTxMetaCache(context.Background(), &hash); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()

				wrappedErr := errors.NewProcessingError("[txmetaHandler][%s] failed to delete tx meta data", hash, err)
				if errors.Is(err, errors.ErrProcessing) {
					// log the wrapped error, instead of throwing an error on Kafka. The message will never be able to be
					// added to the tx meta cache, so we don't want to keep trying to process it.
					u.logger.Warnf(wrappedErr.Error())

					return nil
				}

				return wrappedErr
			} else {
				prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		} else {
			if err := u.SetTxMetaCacheFromBytes(context.Background(), hash.CloneBytes(), msg.Value[chainhash.HashSize:]); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()

				wrappedErr := errors.NewProcessingError("[txmetaHandler][%s] failed to set tx meta data", hash, err)
				if errors.Is(err, errors.ErrProcessing) {
					// log the wrapped error, instead of throwing an error on Kafka. The message will never be able to be
					// added to the tx meta cache, so we don't want to keep trying to process it.
					u.logger.Debugf(wrappedErr.Error())

					return nil
				}

				return wrappedErr
			} else {
				prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		}
	}
	return nil
}
