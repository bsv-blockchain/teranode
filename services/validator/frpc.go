package validator

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
)

type fRPC_Validator struct {
	v *Server
}

func (f *fRPC_Validator) Health(ctx context.Context, message *validator_api.ValidatorApiEmptyMessage) (*validator_api.ValidatorApiHealthResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("Health_frpc").AddTime(start)
	}()

	return &validator_api.ValidatorApiHealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (f *fRPC_Validator) ValidateTransaction(ctx context.Context, req *validator_api.ValidatorApiValidateTransactionRequest) (*validator_api.ValidatorApiValidateTransactionResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("ValidateTransaction_frpc").AddTime(start)
	}()

	prometheusProcessedTransactions.Inc()
	timeStart := time.Now()
	traceSpan := tracing.Start(ctx, "Validator:ValidateTransaction")
	defer traceSpan.Finish()

	tx, err := bt.NewTxFromBytes(req.TransactionData)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		return &validator_api.ValidatorApiValidateTransactionResponse{
			Valid:  false,
			Reason: err.Error(),
		}, fmt.Errorf("cannot read transaction data: %v", err)
	}

	err = f.v.validator.Validate(traceSpan.Ctx, tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		traceSpan.RecordError(err)
		errString := fmt.Sprintf("[ValidateTransaction] transaction %s is invalid: %v", tx.TxID(), err)
		f.v.logger.Errorf(errString)
		return &validator_api.ValidatorApiValidateTransactionResponse{
			Valid:  false,
			Reason: err.Error(),
		}, err
	}

	prometheusTransactionSize.Observe(float64(len(req.TransactionData)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &validator_api.ValidatorApiValidateTransactionResponse{
		Valid: true,
	}, nil
}

func (f *fRPC_Validator) ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidatorApiValidateTransactionBatchRequest) (*validator_api.ValidatorApiValidateTransactionBatchResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		stats.NewStat("ValidateTransactionBatch_frpc").AddTime(start)
	}()

	var err error
	errReasons := make([]*validator_api.ValidatorApiValidateTransactionError, 0, len(req.Transactions))
	for _, reqItem := range req.Transactions {
		r := &validator_api.ValidateTransactionRequest{
			TransactionData: reqItem.TransactionData,
		}
		tx, err := f.v.ValidateTransaction(ctx, r)
		if err != nil {
			errReasons = append(errReasons, &validator_api.ValidatorApiValidateTransactionError{
				TxId:   tx.String(),
				Reason: tx.Reason,
			})
		}
	}

	return &validator_api.ValidatorApiValidateTransactionBatchResponse{
		Valid:   true,
		Reasons: errReasons,
	}, err
}

func (f *fRPC_Validator) ValidateTransactionStream(srv *validator_api.ValidateTransactionStreamServer) error {
	panic("implement me")
}
