package propagation

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/wire"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var (
	propagationStat = gocore.NewStat("propagation")

	// ipv6 multicast constants
	maxDatagramSize = 512 //100 * 1024 * 1024
	ipv6Port        = 9999
)

// PropagationServer type carries the logger within it
type PropagationServer struct {
	status atomic.Uint32
	propagation_api.UnsafePropagationAPIServer
	logger    utils.Logger
	txStore   blob.Store
	validator validator.Interface
}

func Enabled() bool {
	_, found := gocore.Config().Get("utxostore_grpcAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore blob.Store, validatorClient validator.Interface) *PropagationServer {
	initPrometheusMetrics()

	return &PropagationServer{
		logger:    logger,
		txStore:   txStore,
		validator: validatorClient,
	}
}

func (ps *PropagationServer) Init(_ context.Context) (err error) {
	ps.status.Store(1)
	return nil
}

// Start function
func (ps *PropagationServer) Start(ctx context.Context) (err error) {
	ipv6Addresses, ok := gocore.Config().Get("ipv6_addresses")
	if ok {
		err = ps.StartUDP6Listeners(ctx, ipv6Addresses)
		if err != nil {
			return fmt.Errorf("error starting ipv6 listeners: %v", err)
		}
	}

	httpAddress, ok := gocore.Config().Get("propagation_httpAddress")
	if ok {
		var serverURL *url.URL
		serverURL, err = url.Parse(httpAddress)
		if err != nil {
			return fmt.Errorf("HTTP server failed to parse URL [%w]", err)
		}
		err = ps.StartHTTPServer(ctx, serverURL)
		if err != nil {
			return fmt.Errorf("HTTP server failed [%w]", err)
		}
	}

	// Experimental DRPC server - to test throughput at scale
	drpcAddress, ok := gocore.Config().Get("propagation_drpcListenAddress")
	if ok {
		err = ps.drpcServer(ctx, drpcAddress)
		if err != nil {
			ps.logger.Errorf("failed to start DRPC server: %v", err)
		}
	}

	// Experimental fRPC server - to test throughput at scale
	frpcAddress, ok := gocore.Config().Get("propagation_frpcListenAddress")
	if ok {
		err = ps.frpcServer(ctx, frpcAddress)
		if err != nil {
			ps.logger.Errorf("failed to start fRPC server: %v", err)
		}
	}

	ps.status.Store(2)

	// this will block
	if err = util.StartGRPCServer(ctx, ps.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
	}); err != nil {
		return err
	}

	return nil
}

func (ps *PropagationServer) drpcServer(ctx context.Context, drpcAddress string) error {
	ps.logger.Infof("Starting DRPC server on %s", drpcAddress)
	m := drpcmux.New()

	// register the proto-specific methods on the mux
	err := propagation_api.DRPCRegisterPropagationAPI(m, ps)
	if err != nil {
		return fmt.Errorf("failed to register DRPC service: %v", err)
	}
	// create the drpc server
	s := drpcserver.New(m)

	// listen on a tcp socket
	var lis net.Listener
	lis, err = net.Listen("tcp", drpcAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on drpc server: %v", err)
	}

	// run the server
	// N.B.: if you want TLS, you need to wrap the net.Listener with
	// TLS before passing to Serve here.
	go func() {
		err = s.Serve(ctx, lis)
		if err != nil {
			ps.logger.Errorf("failed to serve drpc: %v", err)
		}
	}()

	return nil
}

func (ps *PropagationServer) frpcServer(ctx context.Context, frpcAddress string) error {
	ps.logger.Infof("Starting fRPC server on %s", frpcAddress)

	frpcBa := &fRPC_Propagation{
		ps: ps,
	}

	s, err := propagation_api.NewServer(frpcBa, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create fRPC server: %v", err)
	}

	concurrency, ok := gocore.Config().GetInt("propagation_frpcConcurrency")
	if ok {
		ps.logger.Infof("Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err = s.Start(frpcAddress)
		if err != nil {
			ps.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err = s.Shutdown()
		if err != nil {
			ps.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (ps *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error {
	ps.logger.Infof("Starting UDP6 listeners on %s", ipv6Addresses)

	ipv6Interface, ok := gocore.Config().Get("ipv6_interface", "en0")
	if !ok {
		// default to en0
		ipv6Interface = "en0"
	}

	useInterface, err := net.InterfaceByName(ipv6Interface)
	if err != nil {
		return fmt.Errorf("error resolving interface: %v", err)
	}

	for _, ipv6Address := range strings.Split(ipv6Addresses, ",") {
		var conn *net.UDPConn
		conn, err = net.ListenMulticastUDP("udp6", useInterface, &net.UDPAddr{
			IP:   net.ParseIP(ipv6Address),
			Port: ipv6Port,
			Zone: useInterface.Name,
		})
		if err != nil {
			log.Fatalf("error starting listener: %v", err)
		}

		go func(conn *net.UDPConn) {
			// Loop forever reading from the socket
			//var numBytes int
			var src *net.UDPAddr
			//var oobn int
			//var flags int
			var msg wire.Message
			var b []byte
			var oobB []byte
			var msgTx *wire.MsgExtendedTx

			buffer := make([]byte, maxDatagramSize)
			for {
				_, _, _, src, err = conn.ReadMsgUDP(buffer, oobB)
				if err != nil {
					log.Fatal("ReadFromUDP failed:", err)
				}
				//ps.logger.Infof("read %d bytes from %s, out of bounds data len %d", len(buffer), src.String(), len(oobB))

				reader := bytes.NewReader(buffer)
				msg, b, err = wire.ReadMessage(reader, wire.ProtocolVersion, wire.MainNet)
				if err != nil {
					ps.logger.Errorf("wire.ReadMessage failed: %v", err)
				}
				ps.logger.Infof("read %d bytes into wire message from %s", len(b), src.String())
				//ps.logger.Infof("wire message type: %v", msg)

				msgTx, ok = msg.(*wire.MsgExtendedTx)
				if ok {
					ps.logger.Infof("received %d bytes from %s", len(b), src.String())

					txBytes := bytes.NewBuffer(nil)
					if err = msgTx.Serialize(txBytes); err != nil {
						ps.logger.Errorf("error serializing transaction: %v", err)
						continue
					}

					// Process the received bytes
					go func(txb []byte) {
						if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
							Tx: txb,
						}); err != nil {
							ps.logger.Errorf("error processing transaction: %v", err)
						}
					}(txBytes.Bytes())
				}
			}
		}(conn)
	}

	return nil
}

func (ps *PropagationServer) StartHTTPServer(ctx context.Context, serverURL *url.URL) error {
	// start a simple http listener that handles incoming transaction requests
	http.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
			Tx: body,
		}); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
	})

	go func() {
		if err := http.ListenAndServe(serverURL.Host, nil); err != nil {
			ps.logger.Errorf("HTTP server failed [%s]", err)
		}
	}()

	return nil
}

func (ps *PropagationServer) Stop(_ context.Context) error {
	return nil
}

func (ps *PropagationServer) storeHealth(ctx context.Context) (int, string, error) {
	var sb strings.Builder
	errs := make([]error, 0)

	code, details, err := ps.txStore.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("TxStore: GOOD %d - %q\n", code, details))
	}

	code, details, err = ps.validator.Health(ctx)
	if err != nil {
		errs = append(errs, err)
		_, _ = sb.WriteString(fmt.Sprintf("Validator: BAD %d - %q: %v\n", code, details, err))
	} else {
		_, _ = sb.WriteString(fmt.Sprintf("Validator: GOOD %d - %q\n", code, details))
	}

	if len(errs) > 0 {
		return -1, sb.String(), errors.New("Health errors occurred")
	}

	return 0, sb.String(), nil
}

func (ps *PropagationServer) Health(ctx context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error) {
	start := gocore.CurrentNanos()
	defer func() {
		propagationStat.NewStat("Health").AddTime(start)
	}()

	prometheusHealth.Inc()

	status := ps.status.Load()

	if status != 2 {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   fmt.Sprintf("Propagation server is not ready (Status=%d)", status),
			Timestamp: uint32(time.Now().Unix()),
		}, nil
	}

	code, details, err := ps.storeHealth(ctx)
	if err != nil {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   details,
			Timestamp: uint32(time.Now().Unix()),
		}, err
	}

	if code != 0 {
		return &propagation_api.HealthResponse{
			Ok:        false,
			Details:   details,
			Timestamp: uint32(time.Now().Unix()),
		}, nil
	}

	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (ps *PropagationServer) ProcessTransactionHex(cntxt context.Context, req *propagation_api.ProcessTransactionHexRequest) (*propagation_api.EmptyMessage, error) {
	start := gocore.CurrentNanos()
	stat := propagationStat.NewStat("ProcessTransactionHex")
	defer func() {
		stat.AddTime(start)
	}()
	ctx := util.ContextWithStat(cntxt, stat)

	txBytes, err := hex.DecodeString(req.Tx)
	if err != nil {
		return nil, err
	}

	return ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: txBytes,
	})
}

func (ps *PropagationServer) ProcessTransaction(cntxt context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	start := gocore.CurrentNanos()
	stat := util.StatFromContext(cntxt, propagationStat).NewStat("ProcessTransaction")
	defer func() {
		stat.AddTime(start)
	}()
	ctx := util.ContextWithStat(cntxt, stat)

	prometheusProcessedTransactions.Inc()
	traceSpan := tracing.Start(ctx, "PropagationServer:Set")
	defer traceSpan.Finish()

	timeStart := time.Now()
	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		prometheusInvalidTransactions.Inc()
		return nil, fmt.Errorf("[ProcessTransaction] failed to parse transaction from bytes: %s", err.Error())
	}

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusInvalidTransactions.Inc()
		return nil, fmt.Errorf("[ProcessTransaction][%s] received coinbase transaction", btTx.TxID())
	}

	if !btTx.IsExtended() {
		return nil, fmt.Errorf("[ProcessTransaction][%s] transaction is not extended", btTx.TxID())
	}

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	g, gCtx := errgroup.WithContext(setCtx)
	g.Go(func() error {
		if err = ps.storeTransaction(gCtx, btTx); err != nil {
			return fmt.Errorf("[ProcessTransaction][%s] failed to save transaction: %v", btTx.TxIDChainHash(), err)
		}
		return nil
	})

	if err = ps.validator.Validate(ctx, btTx); err != nil {
		// TODO send REJECT message to peers if invalid tx
		ps.logger.Errorf("[ProcessTransaction][%s] received invalid transaction: %s", btTx.TxID(), err.Error())
		prometheusInvalidTransactions.Inc()
		return nil, err
	}

	if err = g.Wait(); err != nil {
		// TODO: we failed storing the tx in the store, what should we do now?
		//       maybe store in a local badger or a kafka stream and have a process that retries?
		ps.logger.Errorf("[ProcessTransaction][%s] failed to store transaction: %s", btTx.TxID(), err.Error())
	}

	prometheusTransactionSize.Observe(float64(len(req.Tx)))
	prometheusTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) ProcessTransactionStream(stream propagation_api.PropagationAPI_ProcessTransactionStreamServer) error {
	start := gocore.CurrentNanos()
	defer func() {
		propagationStat.NewStat("ProcessTransactionStream").AddTime(start)
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := ps.ProcessTransaction(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (ps *PropagationServer) ProcessTransactionDebug(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	start := gocore.CurrentNanos()
	defer func() {
		propagationStat.NewStat("ProcessTransactionDebug").AddTime(start)
	}()

	btTx, err := bt.NewTxFromBytes(req.Tx)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction from bytes: %s", err.Error())
	}
	if err := ps.storeTransaction(ctx, btTx); err != nil {
		return nil, fmt.Errorf("failed to save transaction %s: %s", btTx.TxIDChainHash().String(), err.Error())
	}
	return &propagation_api.EmptyMessage{}, nil
}

func (ps *PropagationServer) storeTransaction(setCtx context.Context, btTx *bt.Tx) error {
	span, spanCtx := opentracing.StartSpanFromContext(setCtx, "PropagationServer:Set:Store")
	defer span.Finish()

	if err := ps.txStore.Set(spanCtx, btTx.TxIDChainHash().CloneBytes(), btTx.ExtendedBytes()); err != nil {
		// TODO make this resilient to errors
		// write it to secondary store (Kafka, Badger) and retry?
		return err
	}

	return nil
}
