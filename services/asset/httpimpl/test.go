package httpimpl

import (
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/asset/repository"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-p2p/test"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	testTx1, _ = bt.NewTxFromString(test.TX1Raw)

	testBlockHeader = &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      432645644,
		Bits:           model.NBit{},
		Nonce:          12435623,
	}
	testBlockHeaderMeta = &model.BlockHeaderMeta{
		Height:      1,
		TxCount:     2,
		SizeInBytes: 3,
		Miner:       "Miner",
	}
	testBlockInfo = &model.BlockInfo{
		SeenAt:           timestamppb.New(time.Unix(170000000, 0)),
		Height:           123,
		Orphaned:         false,
		BlockHeader:      testBlockHeader.Bytes(),
		Miner:            "miner",
		CoinbaseValue:    321,
		TransactionCount: 2,
		Size:             12345,
	}
	testBlock = &model.Block{
		Header:           testBlockHeader,
		CoinbaseTx:       testTx1,
		TransactionCount: 123,
		SizeInBytes:      321,
		Subtrees:         []*chainhash.Hash{testTx1.TxIDChainHash()},
		SubtreeSlices:    nil,
		Height:           100,
		ID:               666,
	}
	testBlockBytes, _ = testBlock.Bytes()
	testSubtree, _    = util.NewTreeByLeafCount(4)
	testTxMeta        = &meta.Data{
		Tx:          testTx1,
		TxInpoints:  meta.TxInpoints{ParentTxHashes: []chainhash.Hash{*testTx1.Inputs[0].PreviousTxIDChainHash()}, Idxs: [][]uint32{{testTx1.Inputs[0].PreviousTxOutIndex}}},
		BlockIDs:    []uint32{100},
		Fee:         123,
		SizeInBytes: 321,
		IsCoinbase:  false,
		LockTime:    testTx1.LockTime,
	}
	testUtxo = &utxo.SpendResponse{
		Status:       1,
		SpendingData: spendpkg.NewSpendingData(testTx1.TxIDChainHash(), 0),
		LockTime:     100,
	}
)

func init() {
	// add nodes to tree
	for i := uint64(0); i < 4; i++ {
		hash, _ := chainhash.NewHashFromStr(fmt.Sprintf("%x", i))
		_ = testSubtree.AddNode(*hash, i, i)
	}
}

func GetMockHTTP(t *testing.T, body io.Reader) (*HTTP, *repository.Mock, echo.Context, *httptest.ResponseRecorder) {
	// Mock repository
	mockRepo := &repository.Mock{}

	// Create test request
	req, err := http.NewRequest("GET", "/", body)
	if err != nil {
		t.Fatal(err)
	}

	req.RemoteAddr = "example.com"

	// Create test response recorder
	rec := httptest.NewRecorder()

	// Create echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// create random private key
	privateKey, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// Create HTTP handler
	httpServer := &HTTP{
		logger:     ulogger.TestLogger{},
		settings:   &settings.Settings{},
		repository: mockRepo,
		e:          e,
		startTime:  time.Now(),
		privKey:    privateKey,
	}

	return httpServer, mockRepo, c, rec
}
