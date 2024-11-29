package miner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-bitcoin"
)

type Miner struct {
	rpcClient    *bitcoin.Bitcoind
	cpus         int
	coinbaseAddr string
	coinbaseSig  string
}

func NewMiner(rpcClient *bitcoin.Bitcoind, cpus int, coinbaseAddr, coinbaseSig string) *Miner {
	return &Miner{
		rpcClient:    rpcClient,
		cpus:         cpus,
		coinbaseAddr: coinbaseAddr,
		coinbaseSig:  coinbaseSig,
	}
}

func (m *Miner) mine(ctx context.Context) {
	// Start mining
	for {
		select {
		case <-ctx.Done():
			return
		default:
			miningCandidate, err := m.rpcClient.GetMiningCandidate()
			if err != nil {
				fmt.Printf("Error getting mining candidate: %s\n", err)
				os.Exit(1)
			}

			if miningCandidate == nil {
				fmt.Printf("No mining candidate returned from node\n")
				goto end
			}

			fmt.Printf("Candidate %s, on %s\n", miningCandidate.ID, miningCandidate.PreviousHash)

			previousHash, err := chainhash.NewHashFromStr(miningCandidate.PreviousHash)
			if err != nil {
				fmt.Printf("Error parsing previous hash: %s\n", err)
				goto end
			}

			nbits, err := model.NewNBitFromString(miningCandidate.Bits)
			if err != nil {
				fmt.Printf("Error parsing nbits: %s\n", err)
				goto end
			}

			merkleproof := make([][]byte, len(miningCandidate.MerkleProof))

			for i, mp := range miningCandidate.MerkleProof {
				hash, err := chainhash.NewHashFromStr(mp)
				if err != nil {
					fmt.Printf("Error parsing merkle proof: %s\n", err)
					goto end
				}

				merkleproof[i] = hash.CloneBytes()
			}

			candidate := &model.MiningCandidate{
				Id:            []byte(miningCandidate.ID),
				PreviousHash:  previousHash.CloneBytes(),
				CoinbaseValue: miningCandidate.CoinbaseValue,
				Version:       miningCandidate.Version,
				NBits:         nbits.CloneBytes(),
				//nolint:gosec
				Time:        uint32(miningCandidate.CurTime),
				Height:      miningCandidate.Height,
				MerkleProof: merkleproof,
				//nolint:gosec
				SizeWithoutCoinbase: uint32(miningCandidate.SizeWithoutCoinbase),
			}

			solution, rounds, err := m.tryMining(ctx, candidate, m.cpus)
			if err != nil {
				if errors.Is(err, errors.ErrBlockNotFound) {
					fmt.Printf("No solution found for attempt %s after %d rounds\n", miningCandidate.ID, rounds)
					goto end
				}

				fmt.Printf("Error mining block: %s\n", err)
			}

			res, err := m.rpcClient.SubmitMiningSolution(
				string(solution.Id),
				solution.Nonce,
				hex.EncodeToString(solution.Coinbase),
				*solution.Time,
				*solution.Version,
			)
			if err != nil {
				if strings.Contains(err.Error(), "BLOCK SUBMIT FAILED with error: <nil> and result: null") {
					// bug in the go-bitcoin lib, not an actual error, since error and result are both nil
				} else {
					fmt.Printf("Error submitting mining solution: %s\n", err)
					goto end
				}
			}

			fmt.Printf("Submitted solution for %s after %d rounds: %s\n", miningCandidate.ID, rounds, res)
		}

	end:
		time.Sleep(1 * time.Second)

		continue
	}
}

func (m *Miner) tryMining(ctx context.Context, miningCandidate *model.MiningCandidate, threads int) (*model.MiningSolution, uint64, error) {
	// try to mine for 10 seconds
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	solution, rounds, err := m.CPUMine(ctx, miningCandidate, threads)
	if err != nil {
		return nil, rounds, errors.NewProcessingError("error mining block", err)
	}

	if solution == nil {
		return nil, rounds, errors.NewBlockNotFoundError("mine: no solution found after %d rounds", rounds)
	}

	return solution, rounds, nil
}

func (m *Miner) CPUMine(ctx context.Context, candidate *model.MiningCandidate, numThreads int) (*model.MiningSolution, uint64, error) {
	coinbaseTx, err := m.CreateCoinbaseTxCandidate(candidate.Height, candidate.CoinbaseValue)
	if err != nil {
		return nil, 0, err
	}

	rounds := atomic.Uint64{}

	merkleRoot := util.BuildMerkleRootFromCoinbase(coinbaseTx.TxIDChainHash().CloneBytes(), candidate.MerkleProof)

	previousHash, _ := chainhash.NewHash(candidate.PreviousHash)
	merkleRootHash, _ := chainhash.NewHash(merkleRoot)

	nBits, _ := model.NewNBitFromSlice(candidate.NBits)

	//nolint:gosec
	nonceStep := uint32(math.MaxUint32 / numThreads)
	solutionCh := make(chan *model.MiningSolution)
	errCh := make(chan error)

	var wg sync.WaitGroup

	wg.Add(numThreads)

	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer wg.Done()

			//nolint:gosec
			startNonce := uint32(threadID) * nonceStep
			endNonce := startNonce + nonceStep

			var (
				blockHeader = &model.BlockHeader{
					Version:        candidate.Version,
					HashPrevBlock:  previousHash,
					HashMerkleRoot: merkleRootHash,
					Timestamp:      candidate.Time,
					Bits:           *nBits,
				}
				target      = blockHeader.Bits.CalculateTarget()
				headerValid bool
				blockHash   *chainhash.Hash
			)

			for nonce := startNonce; nonce < endNonce; nonce++ {
				select {
				case <-ctx.Done():
					return
				default:
					blockHeader.Nonce = nonce

					rounds.Add(1)

					headerValid, blockHash = HasMetTargetDifficulty(blockHeader, target)
					if headerValid {
						solutionCh <- &model.MiningSolution{
							Id:        candidate.Id,
							Nonce:     nonce,
							Time:      &candidate.Time,
							Coinbase:  coinbaseTx.Bytes(),
							Version:   &candidate.Version,
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
		return solution, rounds.Load(), nil
	case err := <-errCh:
		return nil, rounds.Load(), err
	case <-ctx.Done():
		return nil, rounds.Load(), nil
	}
}

// CreateCoinbaseTxCandidate creates a coinbase transaction for the mining candidate
// p2pk is an optional parameter to specify if the coinbase output should be a pay to public key
// instead of a pay to public key hash
func (m *Miner) CreateCoinbaseTxCandidate(height uint32, coinbaseValue uint64) (*bt.Tx, error) {
	a, b, err := model.GetCoinbaseParts(height, coinbaseValue, m.coinbaseSig, []string{m.coinbaseAddr})
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

	return coinbaseTx, nil
}

var (
	bn   = big.NewInt(0)
	hash *chainhash.Hash
)

func HasMetTargetDifficulty(bh *model.BlockHeader, target *big.Int) (bool, *chainhash.Hash) {
	hash = bh.Hash()

	bn.SetBytes(bt.ReverseBytes(hash[:]))

	if bn.Cmp(target) <= 0 {
		return true, hash
	}

	return false, hash
}
