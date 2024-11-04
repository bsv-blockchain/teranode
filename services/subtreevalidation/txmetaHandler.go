package subtreevalidation

import (
	"bytes"
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (u *Server) txmetaHandler(msg *kafka.KafkaMessage) error {
	if msg != nil && len(msg.Value) > chainhash.HashSize {
		startTime := time.Now()

		// check whether the bytes == delete
		if bytes.Equal(msg.Value[chainhash.HashSize:], []byte("delete")) {
			hash := chainhash.Hash(msg.Value[:chainhash.HashSize])
			if err := u.DelTxMetaCache(context.Background(), &hash); err != nil {
				u.logger.Errorf("failed to delete tx meta data: %v", err)
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				return errors.NewProcessingError("failed to delete tx meta data: %v", err)
			} else {
				prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		} else {
			if err := u.SetTxMetaCacheFromBytes(context.Background(), msg.Value[:chainhash.HashSize], msg.Value[chainhash.HashSize:]); err != nil {
				u.logger.Errorf("failed to set tx meta data: %v", err)
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				return errors.NewProcessingError("failed to set tx meta data: %v", err)
			} else {
				prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		}
	}
	return nil
}
