package cpuminer

import (
	"context"
	"crypto/rand"
	"log"
	"math"
	"runtime"
	"sync"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Mine(ctx context.Context, candidate *model.MiningCandidate) (*model.MiningSolution, error) {
	// Create a new coinbase transaction
	arbitraryText, _ := gocore.Config().Get("coinbase_arbitrary_text", "/TERANODE/")

	coinbasePrivKeys, found := gocore.Config().GetMulti("miner_wallet_private_keys", "|")
	if !found {
		log.Fatal(errors.NewConfigurationError("miner_wallet_private_keys not found in config"))
	}

	multiThreaded := gocore.Config().GetBool("miner_multi_threaded", false)

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := wif.DecodeWIF(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	a, b, err := GetCoinbaseParts(candidate.Height, candidate.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	// The extranonce length is 12 bytes.  We need to add 12 bytes to the coinbase a part
	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	merkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDChainHash().CloneBytes(), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	nBits, _ := model.NewNBitFromSlice(candidate.NBits)

	// to mine with more hashpower
	numThreads := 1
	if multiThreaded {
		numThreads = runtime.NumCPU()
	}

	log.Printf("Mining with %d threads", numThreads)
	nonceStep := uint32(math.MaxUint32 / numThreads)
	solutionCh := make(chan *model.MiningSolution)
	errCh := make(chan error)

	var wg sync.WaitGroup

	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer wg.Done()
			startNonce := uint32(threadID) * nonceStep
			endNonce := startNonce + nonceStep

			for nonce := startNonce; nonce < endNonce; nonce++ {
				select {
				case <-ctx.Done():
					return
				default:
					blockHeader := model.BlockHeader{
						Version:        candidate.Version,
						HashPrevBlock:  previousHash,
						HashMerkleRoot: merkleRootHash,
						Timestamp:      candidate.Time,
						Bits:           *nBits,
						Nonce:          nonce,
					}

					headerValid, blockHash, _ := blockHeader.HasMetTargetDifficulty()
					if headerValid {
						solutionCh <- &model.MiningSolution{
							Id:        candidate.Id,
							Nonce:     nonce,
							Time:      candidate.Time,
							Coinbase:  coinbaseTx.Bytes(),
							Version:   candidate.Version,
							BlockHash: blockHash.CloneBytes(),
						}
						return
					}
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(solutionCh)
		close(errCh)
	}()

	select {
	case solution := <-solutionCh:
		return solution, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, nil
	}
}
