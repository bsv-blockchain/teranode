package blockassembly

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/blockassembly_api"
	"github.com/bitcoin-sv/ubsv/services/blockassembly/subtreeprocessor"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/badger"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	txmeta_store "github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var (
	jobTTL = 10 * time.Minute
)

// BlockAssembly type carries the logger within it
type BlockAssembly struct {
	blockassembly_api.UnimplementedBlockAssemblyAPIServer
	blockAssembler *BlockAssembler
	logger         utils.Logger

	blockchainClient blockchain.ClientI
	txStore          blob.Store
	utxoStore        utxostore.Interface
	txMetaStore      txmeta_store.Store
	subtreeStore     blob.Store
	jobStore         *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job] // has built in locking
	storeUtxoLocal   blob.Store
	storeUtxoCh      map[int]chan *storeUtxos
}

type storeUtxos struct {
	txHash   *chainhash.Hash
	utxos    [][]byte
	locktime uint32
}

func Enabled() bool {
	_, found := gocore.Config().Get("blockassembly_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger utils.Logger, txStore blob.Store, utxoStore utxostore.Interface, txMetaStore txmeta_store.Store,
	subtreeStore blob.Store) *BlockAssembly {

	// initialize Prometheus metrics, singleton, will only happen once
	initPrometheusMetrics()

	localUtxoStore, err := badger.New("./data/blockassembly_storeUtxoLocal")
	if err != nil {
		panic(err)
	}

	storeUtxoCh := make(map[int]chan *storeUtxos)
	for i := 0; i < 256; i++ {
		storeUtxoCh[i] = make(chan *storeUtxos, 100_000)
	}

	ba := &BlockAssembly{
		logger:         logger,
		txStore:        txStore,
		utxoStore:      utxoStore,
		txMetaStore:    txMetaStore,
		subtreeStore:   subtreeStore,
		jobStore:       ttlcache.New[chainhash.Hash, *subtreeprocessor.Job](),
		storeUtxoLocal: localUtxoStore,
		storeUtxoCh:    storeUtxoCh,
	}

	return ba
}

func (ba *BlockAssembly) Init(ctx context.Context) (err error) {
	ba.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		panic(err)
	}

	newSubtreeChan := make(chan *util.Subtree)

	// init the block assembler for this server
	ba.blockAssembler = NewBlockAssembler(ctx, ba.logger, ba.txMetaStore, ba.utxoStore, ba.subtreeStore, ba.blockchainClient, newSubtreeChan)

	// start the new subtree listener in the background
	go func() {
		var subtreeBytes []byte
		for {
			select {
			case <-ctx.Done():
				ba.logger.Infof("Stopping subtree listener")
				return
			case subtree := <-newSubtreeChan:
				prometheusBlockAssemblerSubtreeCreated.Inc()

				if subtreeBytes, err = subtree.Serialize(); err != nil {
					ba.logger.Errorf("Failed to serialize subtree [%s]", err)
					continue
				}

				// TODO context was being canceled, is this hiding a different problem?
				if err = ba.subtreeStore.Set(context.Background(),
					subtree.RootHash()[:],
					subtreeBytes,
					options.WithTTL(120*time.Minute), // this sets the TTL for the subtree, it must be updated when a block is mined
				); err != nil {
					ba.logger.Errorf("Failed to store subtree [%s]", err)
					continue
				}

				if err = ba.blockchainClient.SendNotification(ctx, &model.Notification{
					Type: model.NotificationType_Subtree,
					Hash: subtree.RootHash(),
				}); err != nil {
					ba.logger.Errorf("Failed to send subtree notification [%s]", err)
				}

				ba.logger.Infof("Received new subtree notification for: %s (len %d)", subtree.RootHash().String(), subtree.Length())
			}
		}
	}()

	return nil
}

// Start function
func (ba *BlockAssembly) Start(ctx context.Context) error {

	if err := ba.blockAssembler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start block assembler [%w]", err)
	}

	kafkaBrokersURL, err, ok := gocore.Config().GetURL("blockassembly_kafkaBrokers")
	if err == nil && ok {
		ba.startKafkaListener(ctx, kafkaBrokersURL)
	}

	// Experimental DRPC server - to test throughput at scale
	drpcAddress, ok := gocore.Config().Get("blockassembly_drpcListenAddress")
	if ok {
		err = ba.drpcServer(ctx, drpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to start DRPC server: %v", err)
		}
	}

	// Experimental fRPC server - to test throughput at scale
	frpcAddress, ok := gocore.Config().Get("blockassembly_frpcListenAddress")
	if ok {
		err = ba.frpcServer(ctx, frpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to start fRPC server: %v", err)
		}
	}

	// start the workers to store the utxos in the background
	for i := 0; i < 256; i++ {
		go func(channel chan *storeUtxos) {
			ba.utxoWorker(ctx, channel)
		}(ba.storeUtxoCh[i])
	}

	// TODO start a worker to process utxos that were not updated in the utxo store

	// this will block
	if err = util.StartGRPCServer(ctx, ba.logger, "blockassembly", func(server *grpc.Server) {
		blockassembly_api.RegisterBlockAssemblyAPIServer(server, ba)
	}); err != nil {
		return err
	}

	return nil
}

func (ba *BlockAssembly) utxoWorker(ctx context.Context, channel chan *storeUtxos) {
	for {
		select {
		case <-ctx.Done():
			ba.logger.Infof("Stopping utxo store worker")
			return
		case utxo := <-channel:
			if err := ba.storeUtxos(ctx, utxo.utxos, utxo.locktime); err != nil {
				ba.logger.Errorf("error storing utxos: %s", err)
			} else {
				if err = ba.storeUtxoLocal.Del(ctx, utxo.txHash[:]); err != nil {
					ba.logger.Errorf("failed to delete utxo from local store: %s", err)
				}
			}
		}
	}
}

func (ba *BlockAssembly) drpcServer(ctx context.Context, drpcAddress string) error {
	ba.logger.Infof("Starting DRPC server on %s", drpcAddress)
	m := drpcmux.New()
	// register the proto-specific methods on the mux
	err := blockassembly_api.DRPCRegisterBlockAssemblyAPI(m, ba)
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
			ba.logger.Errorf("failed to serve drpc: %v", err)
		}
	}()

	return nil
}

func (ba *BlockAssembly) frpcServer(ctx context.Context, frpcAddress string) error {
	ba.logger.Infof("Starting fRPC server on %s", frpcAddress)

	frpcBa := &fRPC_BlockAssembly{
		ba: ba,
	}

	s, err := blockassembly_api.NewServer(frpcBa, nil, nil)
	if err != nil {
		return fmt.Errorf("failed to create fRPC server: %v", err)
	}

	concurrency, ok := gocore.Config().GetInt("blockassembly_frpcConcurrency")
	if ok {
		ba.logger.Infof("Setting fRPC server concurrency to %d", concurrency)
		s.SetConcurrency(uint64(concurrency))
	}

	// run the server
	go func() {
		err = s.Start(frpcAddress)
		if err != nil {
			ba.logger.Errorf("failed to serve frpc: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		err = s.Shutdown()
		if err != nil {
			ba.logger.Errorf("failed to shutdown frpc server: %v", err)
		}
	}()

	return nil
}

func (ba *BlockAssembly) startKafkaListener(ctx context.Context, kafkaBrokersURL *url.URL) {
	ba.logger.Infof("[BlockAssembly] Starting Kafka on address: %s", kafkaBrokersURL.String())

	workers, _ := gocore.Config().GetInt("blockassembly_kafkaWorkers", 100)
	ba.logger.Infof("[BlockAssembly] Kafka consumer starting with %d workers", workers)

	// create the workers to process all messages
	n := atomic.Uint64{}
	workerCh := make(chan []byte)
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					ba.logger.Infof("[BlockAssembly] Stopping Kafka worker")
					return
				case txIDBytes := <-workerCh:
					if _, err := ba.AddTx(ctx, &blockassembly_api.AddTxRequest{
						Txid: txIDBytes,
					}); err != nil {
						ba.logger.Errorf("[BlockAssembly] Failed to add tx to block assembly: %s", err)
					} else {
						n.Add(1)
					}
				}
			}
		}()
	}

	go func() {
		clusterAdmin, _, err := util.ConnectToKafka(kafkaBrokersURL)
		if err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to connect to kafka: ", err)
		}
		defer func() { _ = clusterAdmin.Close() }()

		topic := kafkaBrokersURL.Path[1:]
		var partitions int
		if partitions, err = strconv.Atoi(kafkaBrokersURL.Query().Get("partitions")); err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to parse Kafka partitions: ", err)
		}

		var replicationFactor int
		if replicationFactor, err = strconv.Atoi(kafkaBrokersURL.Query().Get("replication")); err != nil {
			ba.logger.Fatalf("[BlockAssembly] unable to parse Kafka replication factor: ", err)
		}

		_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replicationFactor),
		}, false)

		if err = util.StartKafkaGroupListener(ctx, ba.logger, kafkaBrokersURL, "validators", workerCh); err != nil {
			ba.logger.Errorf("[BlockAssembly] Kafka listener failed to start: %s", err)
		}
	}()
}

func (ba *BlockAssembly) Stop(_ context.Context) error {
	return nil
}

func (ba *BlockAssembly) Health(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error) {
	prometheusBlockAssemblyHealth.Inc()

	return &blockassembly_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error) {
	startTime := time.Now()
	traceSpan := tracing.Start(ctx, "BlockAssembly:AddTx")

	prometheusBlockAssemblyAddTx.Inc()
	defer func() {
		traceSpan.Finish()
		prometheusBlockAssemblerTransactions.Set(float64(ba.blockAssembler.TxCount()))
		prometheusBlockAssemblyAddTxDuration.Observe(time.Since(startTime).Seconds())
	}()

	txHash, err := chainhash.NewHash(req.Txid)
	if err != nil {
		return nil, err
	}

	addTxSpan, _ := opentracing.StartSpanFromContext(ctx, "BlockAssembly:AddTx:addTx")
	if err = ba.blockAssembler.AddTx(txHash, req.Fee, req.Size); err != nil {
		return nil, err
	}
	addTxSpan.Finish()

	utxoLength := len(req.Utxos)
	if utxoLength > 0 {
		lengthVarInt := bt.VarInt(uint64(utxoLength))
		utxoBytes := make([]byte, 0, len(req.Utxos)+lengthVarInt.Length())
		utxoBytes = append(utxoBytes, lengthVarInt.Bytes()...)
		for _, utxo := range req.Utxos {
			utxoBytes = append(utxoBytes, utxo...)
		}

		// this sets the TTL for the utxo, it will be purged in 2 hours
		if err = ba.storeUtxoLocal.Set(ctx, txHash[:], utxoBytes, options.WithTTL(120*time.Minute)); err != nil {
			ba.logger.Errorf("failed to store utxo locally: %s", err)
		}
	}

	ba.storeUtxoCh[int(txHash[0])] <- &storeUtxos{
		txHash:   txHash,
		utxos:    req.Utxos,
		locktime: req.Locktime,
	}

	return &blockassembly_api.AddTxResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) storeUtxos(ctx context.Context, utxoBytes [][]byte, locktime uint32) error {
	startUtxoTime := time.Now()
	utxoSpan, utxoSpanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:AddTx:utxo")
	defer func() {
		utxoSpan.Finish()
		prometheusBlockAssemblerUtxoStoreDuration.Observe(time.Since(startUtxoTime).Seconds())
	}()

	var err error
	utxoHashes := make([]*chainhash.Hash, len(utxoBytes))
	for i, hashBytes := range utxoBytes {
		utxoHashes[i], err = chainhash.NewHash(hashBytes)
		if err != nil {
			return err
		}
	}

	// Add all the utxo hashes to the utxostore
	var resp *utxostore.UTXOResponse
	for _, hash := range utxoHashes {
		if resp, err = ba.utxoStore.Store(utxoSpanCtx, hash, locktime); err != nil {
			return fmt.Errorf("error storing utxo (%v): %w", resp, err)
		}
	}

	return nil
}

func (ba *BlockAssembly) GetTxMeta(ctx context.Context, txHash *chainhash.Hash) (*txmeta_store.Data, error) {
	startMetaTime := time.Now()
	txMetaSpan, txMetaSpanCtx := opentracing.StartSpanFromContext(ctx, "BlockAssembly:AddTx:txMeta")
	defer func() {
		txMetaSpan.Finish()
		prometheusBlockAssemblerTxMetaGetDuration.Observe(time.Since(startMetaTime).Seconds())
	}()

	txMetadata, err := ba.txMetaStore.Get(txMetaSpanCtx, txHash)
	if err != nil {
		return nil, err
	}

	currentChainMap := ba.blockAssembler.GetCurrentChainMap()

	// looking this up here and adding to the subtree processor, might create a situation where a transaction
	// that was in a block from a competing miner, is added to the subtree processor when it shouldn't
	if len(txMetadata.BlockHashes) > 0 {
		for _, hash := range txMetadata.BlockHashes {
			if _, ok := currentChainMap[*hash]; ok {
				// the tx is already in a block on our chain, nothing to do
				return nil, fmt.Errorf("tx already in a block on the active chain: %s", hash)
			}
		}
	}

	return txMetadata, nil
}

func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*model.MiningCandidate, error) {
	startTime := time.Now()
	prometheusBlockAssemblyGetMiningCandidate.Inc()

	miningCandidate, subtrees, err := ba.blockAssembler.GetMiningCandidate(ctx)
	if err != nil {
		return nil, err
	}

	id, _ := chainhash.NewHash(miningCandidate.Id)
	ba.jobStore.Set(*id, &subtreeprocessor.Job{
		ID:              id,
		Subtrees:        subtrees,
		MiningCandidate: miningCandidate,
	}, jobTTL) // create a new job with a TTL, will be cleaned up automatically

	prometheusBlockAssemblyGetMiningCandidateDuration.Observe(time.Since(startTime).Seconds())

	return miningCandidate, nil
}

func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error) {
	startTime := time.Now()

	prometheusBlockAssemblySubmitMiningSolution.Inc()
	ba.logger.Infof("[BlockAssembly] SubmitMiningSolution: %x", req.Id)

	storeId, err := chainhash.NewHash(req.Id[:])
	if err != nil {
		return nil, err
	}

	jobItem := ba.jobStore.Get(*storeId)
	if jobItem == nil {
		return nil, fmt.Errorf("[BlockAssembly] job not found")
	}
	job := jobItem.Value()

	hashPrevBlock, err := chainhash.NewHash(job.MiningCandidate.PreviousHash)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly] failed to convert hashPrevBlock: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(req.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("[BlockAssembly] failed to convert coinbaseTx: %w", err)
	}
	coinbaseTxIDHash := coinbaseTx.TxIDChainHash()

	var sizeInBytes uint64

	subtreesInJob := make([]*util.Subtree, len(job.Subtrees))
	subtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	jobSubtreeHashes := make([]*chainhash.Hash, len(job.Subtrees))
	transactionCount := uint64(0)
	if len(job.Subtrees) > 0 {
		for i, subtree := range job.Subtrees {
			// the job subtree hash needs to be stored for the block, before the coinbase is replaced in the first
			// subtree, which changes the id of the subtree
			jobSubtreeHashes[i] = subtree.RootHash()

			if i == 0 {
				subtreesInJob[i] = subtree.Duplicate()
				subtreesInJob[i].ReplaceRootNode(coinbaseTxIDHash, 0, uint64(coinbaseTx.Size()))
			} else {
				subtreesInJob[i] = subtree
			}

			rootHash := subtreesInJob[i].RootHash()
			subtreeHashes[i], _ = chainhash.NewHash(rootHash[:])

			transactionCount += uint64(subtree.Length())
			sizeInBytes += subtree.SizeInBytes
		}
	} else {
		transactionCount = 1 // Coinbase
		sizeInBytes = uint64(coinbaseTx.Size())
	}

	// Create a new subtree with the subtreeHashes of the subtrees
	topTree := util.NewTreeByLeafCount(util.CeilPowerOfTwo(len(subtreesInJob)))
	for _, hash := range subtreeHashes {
		err = topTree.AddNode(hash, 1, 0)
		if err != nil {
			return nil, err
		}
	}

	var hashMerkleRoot *chainhash.Hash
	var coinbaseMerkleProof []*chainhash.Hash

	if len(subtreesInJob) == 0 {
		hashMerkleRoot = coinbaseTxIDHash
	} else {
		coinbaseMerkleProof, err = util.GetMerkleProofForCoinbase(subtreesInJob)
		if err != nil {
			return nil, fmt.Errorf("[BlockAssembly] error getting merkle proof for coinbase: %w", err)
		}

		cmp := make([]string, len(coinbaseMerkleProof))
		cmpB := make([][]byte, len(coinbaseMerkleProof))
		for idx, hash := range coinbaseMerkleProof {
			cmp[idx] = hash.String()
			cmpB[idx] = hash.CloneBytes()
		}

		calculatedMerkleRoot := topTree.RootHash()
		hashMerkleRoot, err = chainhash.NewHash(calculatedMerkleRoot[:])
		if err != nil {
			return nil, err
		}
	}

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        req.Version,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      req.Time,
			Bits:           model.NewNBitFromSlice(job.MiningCandidate.NBits),
			Nonce:          req.Nonce,
		},
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes + 80 + util.VarintSize(transactionCount), // 80 byte header and bytes for txcount, // TODO calculate varint of transaction count
		Subtrees:         jobSubtreeHashes,                                     // we need to store the hashes of the subtrees in the block, without the coinbase
	}

	ba.logger.Infof("[BlockAssembly] validating block: %s", block.Header.Hash())
	// check fully valid, including whether difficulty in header is low enough
	if ok, err := block.Valid(ctx, nil, nil, nil); !ok {
		ba.logger.Errorf("[BlockAssembly] invalid block: %s - %v - %v", utils.ReverseAndHexEncodeHash(*block.Header.Hash()), block.Header, err)
		return nil, fmt.Errorf("[BlockAssembly] invalid block: %v", err)
	}

	ba.logger.Infof("[BlockAssembly] add block to blockchain: %s", block.Header.Hash())
	// add block to the blockchain
	if err = ba.blockchainClient.AddBlock(ctx, block); err != nil {
		return nil, fmt.Errorf("failed to add block: %w", err)
	}

	// TODO context was being canceled, is this hiding a different problem?
	err = ba.txStore.Set(context.Background(), block.CoinbaseTx.TxIDChainHash().CloneBytes(), block.CoinbaseTx.ExtendedBytes())
	if err != nil {
		ba.logger.Errorf("[BlockAssembly] error storing coinbase tx in tx store: %v", err)
	}

	// decouple the tracing context to not cancel the context when the subtree TTL is being saved in the background
	callerSpan := opentracing.SpanFromContext(ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)

	if err = ba.updateSubtreesTTL(setCtx, block); err != nil {
		ba.logger.Errorf("failed to update subtrees TTL: %w", err)
	}

	// add the transactions in this block to the txMeta block hashes
	if err = UpdateTxMinedStatus(ctx, ba.txMetaStore, subtreesInJob, block.Header); err != nil {
		// TODO this should be a fatal error, but for now we just log it
		//return nil, fmt.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
		ba.logger.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
	}

	// remove jobs, we have already mined a block
	// if we don't do this, all the subtrees will never be removed from memory
	ba.jobStore.DeleteAll()

	prometheusBlockAssemblySubmitMiningSolutionDuration.Observe(time.Since(startTime).Seconds())

	return &blockassembly_api.SubmitMiningSolutionResponse{
		Ok: true,
	}, nil
}

func (ba *BlockAssembly) updateSubtreesTTL(setCtx context.Context, block *model.Block) (err error) {
	starTime := time.Now()
	span, spanCtx := opentracing.StartSpanFromContext(setCtx, "BlockAssembly:SubmitMiningSolution:updateSubtreesTTL")
	defer func() {
		span.Finish()
		prometheusBlockAssemblyUpdateSubtreesTTL.Observe(time.Since(starTime).Seconds())
	}()

	// update the subtree TTLs
	for _, subtreeHash := range block.Subtrees {
		go func(subtreeHashBytes []byte) {
			err = ba.subtreeStore.SetTTL(spanCtx, subtreeHashBytes, 0)
			if err != nil {
				ba.logger.Errorf("failed to update subtree TTL: %w", err)
			}
		}(subtreeHash[:])
	}

	return nil
}

func UpdateTxMinedStatus(ctx context.Context, txMetaStore txmeta_store.Store, subtrees []*util.Subtree, blockHeader *model.BlockHeader) error {
	startTime := time.Now()
	defer func() {
		prometheusBlockAssemblyUpdateTxMinedStatus.Observe(time.Since(startTime).Seconds())
	}()

	g, gCtx := errgroup.WithContext(ctx)

	for _, subtree := range subtrees {
		nodes := subtree.Nodes
		g.Go(func() error {
			for _, node := range nodes {
				if err := txMetaStore.SetMined(gCtx, node.Hash, blockHeader.Hash()); err != nil {
					return fmt.Errorf("[BlockAssembly] error setting mined tx: %v", err)
				}
			}

			return nil
		})
	}

	return nil
}
