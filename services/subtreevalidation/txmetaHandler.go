package subtreevalidation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (u *Server) txmetaHandler(msg util.KafkaMessage) {
	if msg.Message != nil && len(msg.Message.Value) > chainhash.HashSize {
		startTime := time.Now()

		if err := u.SetTxMetaCacheFromBytes(context.Background(), msg.Message.Value[:chainhash.HashSize], msg.Message.Value[chainhash.HashSize:]); err != nil {
			u.logger.Errorf("failed to set tx meta data: %v", err)
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
		} else {
			prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
		}
	}
}
