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

		hash := chainhash.Hash(msg.Value[:chainhash.HashSize])

		// check whether the bytes == delete
		if bytes.Equal(msg.Value[chainhash.HashSize:], []byte("delete")) {
			if err := u.DelTxMetaCache(context.Background(), &hash); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				return errors.NewProcessingError("[txmetaHandler][%s] failed to delete tx meta data", hash, err)
			} else {
				prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		} else {
			if err := u.SetTxMetaCacheFromBytes(context.Background(), hash.CloneBytes(), msg.Value[chainhash.HashSize:]); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				return errors.NewProcessingError("[txmetaHandler][%s] failed to set tx meta data", hash, err)
			} else {
				prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		}
	}
	return nil
}
