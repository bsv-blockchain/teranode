package blaster

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/services/propagation/propagation_api"
	seeder_api "github.com/TAAL-GmbH/ubsv/services/seeder/seeder_api"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Blaster struct {
	logger     utils.Logger
	seeder     seeder_api.SeederAPIClient
	propagator propagation_api.PropagationAPIClient
}

func Enabled() bool {
	return gocore.Config().GetBool("blaster_enabled")
}

func New() *Blaster {
	seederGrpcAddress, ok := gocore.Config().Get("seeder_grpcAddress")
	if !ok {
		panic("no seeder_grpcAddress setting found")
	}

	sConn, err := utils.GetGRPCClient(context.Background(), seederGrpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	propagationGrpcAddress, ok := gocore.Config().Get("propagation_grpcAddress")
	if !ok {
		panic("no propagation_grpcAddress setting found")
	}

	pConn, err := utils.GetGRPCClient(context.Background(), propagationGrpcAddress, &utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	return &Blaster{
		logger:     gocore.Log("blaster", gocore.NewLogLevelFromString("debug")),
		seeder:     seeder_api.NewSeederAPIClient(sConn),
		propagator: propagation_api.NewPropagationAPIClient(pConn),
	}
}

// Start function
func (b *Blaster) Start() error {
	for {
		ctx := context.Background()

		res, err := b.seeder.NextSpendableTransaction(ctx, &emptypb.Empty{})
		if err != nil {
			b.logger.Errorf("Could not get next spendable output: %v", err)
			return err
		}

		b.logger.Infof("Received transaction %x with %d outputs", res.Txid, res.NumberOfOutputs)

		privateKey, publicKey := bec.PrivKeyFromBytes(bec.S256(), res.PrivateKey)

		script, err := bscript.NewP2PKHFromPubKeyBytes(publicKey.SerialiseCompressed())
		if err != nil {
			b.logger.Errorf("Could not create locking script: %v", err)
			return err
		}

		// Make a new transaction
		tx := bt.NewTx()

		if err := tx.FromUTXOs(&bt.UTXO{
			TxID:          res.Txid,
			Vout:          0,
			LockingScript: script,
			Satoshis:      res.SatoshisPerOutput,
		}); err != nil {
			b.logger.Errorf("Could not add UTXO to transaction: %v", err)
			return err
		}

		if err := tx.PayTo(script, res.SatoshisPerOutput); err != nil {
			b.logger.Errorf("Could not add output to transaction: %v", err)
			return err
		}

		unlockerGetter := unlocker.Getter{PrivateKey: privateKey}

		for i := uint32(0); i < res.NumberOfOutputs; i++ {
			tx.Inputs[0].PreviousTxOutIndex = i

			if err := tx.FillAllInputs(context.Background(), &unlockerGetter); err != nil {
				b.logger.Errorf("Could not sign transaction: %v", err)
				return err
			}

			if _, err := b.propagator.Set(ctx, &propagation_api.SetRequest{
				Tx: tx.ExtendedBytes(),
			}); err != nil {
				b.logger.Errorf("Failed to send tx to propagation service: %v", err)
			}
		}
	}
}

func (v *Blaster) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

}
