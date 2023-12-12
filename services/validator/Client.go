package validator

import (
	"context"
	"net"
	"strings"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
	"storj.io/drpc/drpcconn"
)

type Client struct {
	client       validator_api.ValidatorAPIClient
	drpcClient   validator_api.DRPCValidatorAPIClient
	frpcClient   *validator_api.Client
	running      bool
	conn         *grpc.ClientConn
	logger       ulogger.Logger
	batchCh      chan *validator_api.ValidateTransactionRequest
	batchSize    int
	batchTimeout int
}

func NewClient(ctx context.Context, logger ulogger.Logger) (*Client, error) {

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}

	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, validator_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	grpcClient := validator_api.NewValidatorAPIClient(conn)

	sendBatchSize, _ := gocore.Config().GetInt("validator_sendBatchSize", 0)
	sendBatchTimeout, _ := gocore.Config().GetInt("validator_sendBatchTimeout", 100)
	sendBatchWorkers, _ := gocore.Config().GetInt("validator_sendBatchWorkers", 1)

	if sendBatchSize > 0 && sendBatchWorkers <= 0 {
		logger.Fatalf("expecting validator_sendBatchWorkers > 0 when validator_sendBatchSize = %d", sendBatchSize)
	}

	client := &Client{
		client:       grpcClient,
		logger:       logger,
		running:      true,
		conn:         conn,
		batchCh:      make(chan *validator_api.ValidateTransactionRequest),
		batchSize:    sendBatchSize,
		batchTimeout: sendBatchTimeout,
	}

	// Connect to experimental DRPC server if configured
	client.connectDRPC()

	// Connect to experimental fRPC server if configured
	// fRPC has only been implemented for AddTx / Store
	client.connectFRPC()

	if sendBatchSize > 0 {
		for i := 0; i < sendBatchWorkers; i++ {
			go client.batchWorker(ctx)
		}
	}

	if client.frpcClient != nil {
		/* listen for close channel and reconnect */
		client.logger.Infof("Listening for close channel on fRPC client")
		go func() {
			for {
				select {
				case <-ctx.Done():
					client.logger.Infof("fRPC client context done, closing channel")
					return
				case <-client.frpcClient.CloseChannel():
					client.logger.Infof("fRPC client close channel received, reconnecting...")
					client.connectFRPC()
				}
			}
		}()
	}

	return client, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) Health(ctx context.Context) (int, string, error) {
	_, err := c.client.Health(ctx, &validator_api.EmptyMessage{})
	if err != nil {
		return -1, "Validator", err
	}

	return 0, "Validator", nil
}

func (c *Client) GetBlockHeight() (uint32, error) {
	resp, err := c.client.GetBlockHeight(context.Background(), &validator_api.EmptyMessage{})
	if err != nil {
		return 0, err
	}

	return resp.Height, nil
}

func (c *Client) Validate(ctx context.Context, tx *bt.Tx) error {
	if c.batchSize == 0 {
		if c.frpcClient != nil {

			if _, err := c.frpcClient.ValidatorAPI.ValidateTransaction(ctx, &validator_api.ValidatorApiValidateTransactionRequest{
				TransactionData: tx.ExtendedBytes(),
			}); err != nil {
				return err
			}

		} else if c.drpcClient != nil {

			if _, err := c.drpcClient.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
				TransactionData: tx.ExtendedBytes(),
			}); err != nil {
				return err
			}

		} else {

			if _, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
				TransactionData: tx.ExtendedBytes(),
			}); err != nil {
				return err
			}

		}
	} else {

		/* batch mode */
		c.batchCh <- &validator_api.ValidateTransactionRequest{
			TransactionData: tx.ExtendedBytes(),
		}

	}

	return nil
}

func (c *Client) batchWorker(ctx context.Context) {
	duration := time.Duration(c.batchTimeout) * time.Millisecond
	ringBuffer := make([]*validator_api.ValidateTransactionRequest, c.batchSize)
	i := 0
	for {
		select {
		case req := <-c.batchCh:
			ringBuffer[i] = req
			i++
			if i == c.batchSize {
				c.sendBatchToValidator(ctx, ringBuffer)
				i = 0
			}
		case <-time.After(duration):
			if i > 0 {
				c.sendBatchToValidator(ctx, ringBuffer[:i])
				i = 0
			}
		}
	}
}

func (c *Client) sendBatchToValidator(ctx context.Context, batch []*validator_api.ValidateTransactionRequest) {
	if c.frpcClient != nil {

		fBatch := make([]*validator_api.ValidatorApiValidateTransactionRequest, len(batch))
		for i, req := range batch {
			fBatch[i] = &validator_api.ValidatorApiValidateTransactionRequest{
				TransactionData: req.GetTransactionData(),
			}
		}
		txBatch := &validator_api.ValidatorApiValidateTransactionBatchRequest{
			Transactions: fBatch,
		}

		resp, err := c.frpcClient.ValidatorAPI.ValidateTransactionBatch(ctx, txBatch)
		if err != nil {
			c.logger.Errorf("%v", err)
			return
		}
		if len(resp.Reasons) > 0 {
			c.logger.Errorf("batch send to validator returned %d failed transactions from %d batch", len(resp.Reasons), len(batch))
		}

	} else if c.drpcClient != nil {

		txBatch := &validator_api.ValidateTransactionBatchRequest{
			Transactions: batch,
		}

		resp, err := c.drpcClient.ValidateTransactionBatch(ctx, txBatch)
		if err != nil {
			c.logger.Errorf("%v", err)
			return
		}
		if len(resp.Reasons) > 0 {
			c.logger.Errorf("batch send to validator returned %d failed transactions from %d batch", len(resp.Reasons), len(batch))
		}

	} else {

		txBatch := &validator_api.ValidateTransactionBatchRequest{
			Transactions: batch,
		}
		resp, err := c.client.ValidateTransactionBatch(ctx, txBatch)
		if err != nil {
			c.logger.Errorf("%v", err)
			return
		}
		if len(resp.Reasons) > 0 {
			c.logger.Errorf("batch send to validator returned %d failed transactions from %d batch", len(resp.Reasons), len(batch))
		}

	}
}

func (c *Client) connectDRPC() {
	func() {
		err := recover()
		if err != nil {
			c.logger.Errorf("Error connecting to validator DRPC: %s", err)
		}
	}()

	validatorDrpcAddress, ok := gocore.Config().Get("validator_drpcAddress")
	if ok {
		c.logger.Infof("Using DRPC connection to validator")
		time.Sleep(5 * time.Second) // allow everything to come up and find a better way to do this
		rawConn, err := net.Dial("tcp", validatorDrpcAddress)
		if err != nil {
			c.logger.Errorf("Error connecting to validator: %s", err)
		}
		conn := drpcconn.New(rawConn)
		c.drpcClient = validator_api.NewDRPCValidatorAPIClient(conn)
		c.logger.Infof("Connected to validator DRPC server")
	}
}

func (c *Client) connectFRPC() {
	func() {
		err := recover()
		if err != nil {
			c.logger.Errorf("Error connecting to validator fRPC: %s", err)
		}
	}()

	validatorFRPCAddress, ok := gocore.Config().Get("validator_frpcAddress")
	if ok {
		maxRetries := 5
		retryInterval := 5 * time.Second

		for i := 0; i < maxRetries; i++ {
			c.logger.Infof("Attempting to create fRPC connection to validator, attempt %d", i+1)

			client, err := validator_api.NewClient(nil, nil)
			if err != nil {
				c.logger.Fatalf("Error creating new fRPC client in validator: %s", err)
			}

			err = client.Connect(validatorFRPCAddress)
			if err != nil {
				c.logger.Infof("Error connecting to fRPC server in validator: %s", err)
				if i+1 == maxRetries {
					break
				}
				time.Sleep(retryInterval)
				retryInterval *= 2
			} else {
				c.logger.Infof("Connected to validator fRPC server")
				c.frpcClient = client
				break
			}
		}

		if c.frpcClient == nil {
			c.logger.Fatalf("Failed to connect to validator fRPC server after %d attempts", maxRetries)
		}

	}
}

func (c *Client) Subscribe(ctx context.Context, source string) (chan *model.RejectedTxNotification, error) {
	ch := make(chan *model.RejectedTxNotification)

	go func() {
		<-ctx.Done()
		c.logger.Infof("[Asset] context done, closing subscription: %s", source)
		c.running = false
		err := c.conn.Close()
		if err != nil {
			c.logger.Errorf("[Asset] failed to close connection", err)
		}
	}()

	go func() {
		defer close(ch)

		for c.running {
			stream, err := c.client.Subscribe(ctx, &validator_api.SubscribeRequest{
				Source: source,
			})
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}

			for c.running {
				resp, err := stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("[Validator] failed to receive notification: %v", err)
					}
					time.Sleep(1 * time.Second)
					break
				}

				c.logger.Debugf("[Validator] received notification %+v", resp)
				ch <- &model.RejectedTxNotification{
					TxId:   resp.TxId,
					Reason: resp.Reason,
				}
			}
		}
	}()

	return ch, nil
}
