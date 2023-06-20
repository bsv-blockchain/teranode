package worker

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TAAL-GmbH/ubsv/cmd/txblaster/extra"
	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	"github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/libsv/go-bt/v2"
	"golang.org/x/time/rate"
)

func TestWorker_fireTransactions(t *testing.T) {
	type fields struct {
		utxoChan             chan *bt.UTXO
		startTime            time.Time
		numberOfOutputs      int
		numberOfTransactions uint32
		satoshisPerOutput    uint64
		seeder               seeder_api.SeederAPIClient
		rateLimiter          *rate.Limiter
		propagationServer    propagation_api.PropagationAPIClient
		kafkaProducer        sarama.SyncProducer
		kafkaTopic           string
		printProgress        uint64
	}
	type args struct {
		ctx    context.Context
		u      *bt.UTXO
		keySet *extra.KeySet
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				utxoChan:             tt.fields.utxoChan,
				startTime:            tt.fields.startTime,
				numberOfOutputs:      tt.fields.numberOfOutputs,
				numberOfTransactions: tt.fields.numberOfTransactions,
				satoshisPerOutput:    tt.fields.satoshisPerOutput,
				seeder:               tt.fields.seeder,
				rateLimiter:          tt.fields.rateLimiter,
				propagationServer:    tt.fields.propagationServer,
				kafkaProducer:        tt.fields.kafkaProducer,
				kafkaTopic:           tt.fields.kafkaTopic,
				printProgress:        tt.fields.printProgress,
			}
			if err := w.fireTransactions(tt.args.ctx, tt.args.u, tt.args.keySet); (err != nil) != tt.wantErr {
				t.Errorf("Worker.fireTransactions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
