package model

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	block1       = "0000002006226e46111a0b59caaf126043eb5bbf28c34f3a5e332a1fc7b2b73cf188910f1633819a69afbd7ce1f1a01c3b786fcbb023274f3b15172b24feadd4c80e6c6a8b491267ffff7f20040000000102000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000"
	block1Header = block1[:160]
)

// The following JSON strings are used to test the NewBlockHeaderFromJSON function
// They are taken from the Bitcoin SV regtest and the candidate was taken just before the block was mined,
// so the candidate and block are related.
var (
	getMiningCandidateJSON = `{
		"id": "b64176f4aada053a8d22b7d2755bfcf47793dc097dd87f2a69e6c6fb98cbf2a1",
		"prevhash": "1a270fe33eab79fef413754296ae329b4fd3979feb8b8657597c54371ae524a3",
		"coinbase": "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff06037886000101ffffffff01a82f000000000000232103a920b957d6d2268812e02dfd8799ed2a867e2df86c4f8d1eaecb4c35266692b5ac00000000",
		"coinbaseValue": 12200,
		"version": 536870912,
		"nBits": "207fffff",
		"time": 1731944075,
		"height": 34424,
		"num_tx": 4,
		"sizeWithoutCoinbase": 690,
		"merkleProof": [
			"9f0a5462ca027f74b8c8e872331da1a55520197ff8734b604505c93cc7dfb968",
			"11a375f3e547d4babb672471a167443f96077c7c9950548ce6ec460f6da37a32"
		]
	}`
	submitMiningSolutionJSON = `{
		"id": "b64176f4aada053a8d22b7d2755bfcf47793dc097dd87f2a69e6c6fb98cbf2a1",
		"version": 536870912,
		"time": 1731944075,
		"coinbase": "02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff06037886000101ffffffff01a82f000000000000232103a920b957d6d2268812e02dfd8799ed2a867e2df86c4f8d1eaecb4c35266692b5ac00000000",
		"nonce": 1
	}`
	getBlockJSON = `{
  "tx": [
    "b2d725550ba419ef7452626f75faa8538bca695ab9284127b2210368455137d1",
    "9f0a5462ca027f74b8c8e872331da1a55520197ff8734b604505c93cc7dfb968",
    "c12d4c884f68728bbb119836bb07116d737752e5e775eb8a1338b572fd6489df",
    "69a969fbbd7a9ca611382154bf4cc2c4dab6756d36612c5f74cfbb4641826f12"
  ],
  "hash": "611fd97881064670555ac01db182c46134e770aa47d1a794b7df2767e42f3f89",
  "confirmations": 1,
  "size": 792,
  "height": 34424,
  "version": 536870912,
  "versionHex": "20000000",
  "merkleroot": "69813d58079d5d2924cf62b9f183bc058c04a98e35e67060dcfb71ad5435cb8a",
  "num_tx": 4,
  "time": 1731944075,
  "mediantime": 1731943997,
  "nonce": 1,
  "bits": "207fffff",
  "difficulty": 4.656542373906925e-10,
  "chainwork": "0000000000000000000000000000000000000000000000000000000000010cf2",
  "previousblockhash": "1a270fe33eab79fef413754296ae329b4fd3979feb8b8657597c54371ae524a3"
}`

	blockBytes, _ = hex.DecodeString("00000020a324e51a37547c5957868beb9f97d34f9b32ae96427513f4fe79ab3ee30f271a8acb3554ad71fbdc6070e6358ea9048c05bc83f1b962cf24295d9d07583d81698b5e3b67ffff7f20010000000402000000010000000000000000000000000000000000000000000000000000000000000000ffffffff06037886000101ffffffff01a82f000000000000232103a920b957d6d2268812e02dfd8799ed2a867e2df86c4f8d1eaecb4c35266692b5ac000000000200000001afb41c129af22ca5c05cc677993e7d8e040b2610baaca5778e7f71549fa74b89010000006b483045022100914fac419890679f1f4ba2efe22ac9721416283f4fd150f0af169026056d2f780220109a8787d494d9aa71ac0198651458f4930cb998ed6029c0221a42e2044470334121030cfa8aaa20d16e6c1f8e42ca3a0a80c6b9496d2fa39182d7ea9a0c44298c6877feffffff0200e1f505000000001976a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac80d7b0c4000000001976a91432dd05fe95dbc4172cc6b8335f180cdd987f278588ac778600000200000001a11489634e961ebed5143033c539675cb0682fb30d4b42e2b3ff3b71f01f359b0000000049483045022100f051603a90395cd56ab752a1124838d18d1f56d5382889c22aaf298ee6b0cc89022046a23b5d21f45bba54bf3e33942c9305c3507f0da3aa16cc2ca869d3377ca7b441feffffff0200e1f505000000001976a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac00021024010000001976a91442f37d99df083ec79802c38e00a21fe6b1f4583588ac778600000200000001dc1011b70ec59e1e0d24d15018fae10e0428d03ced79d2bcdf855ecf3b4f1ff700000000494830450221008de2576427d3cdada7037dcc739391ed5a732b3b02fe727942d703bc2c9c4abf02201b2a563f313914727523f81890566a25bb760b21d704c8a5747b5a9847fb450e41feffffff0200021024010000001976a9144fb3e816665c1daf8130ba9bc446b29e15b1f83788ac00e1f505000000001976a91462e907b15cbf27d5425399ebf6f0fb50ebb88f1888ac77860000")
)

func TestNewBlockHeaderFromBytes(t *testing.T) {
	t.Run("block 1 from bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)

		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
		assert.Equal(t, uint32(0x20000000), blockHeader.Version)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1729251723), blockHeader.Timestamp)
		assert.Equal(t, "207fffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(4), blockHeader.Nonce)
	})

	t.Run("block 0 from string", func(t *testing.T) {
		blockHeader, err := NewBlockHeaderFromString(block1Header)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, uint32(0x20000000), blockHeader.Version)
		assert.Equal(t, "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206", blockHeader.HashPrevBlock.String())
		assert.Equal(t, "6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316", blockHeader.HashMerkleRoot.String())
		assert.Equal(t, uint32(1729251723), blockHeader.Timestamp)
		assert.Equal(t, "207fffff", blockHeader.Bits.String())
		assert.Equal(t, uint32(4), blockHeader.Nonce)
	})

	t.Run("block 1 bytes", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)

		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, blockHeaderBytes, blockHeader.Bytes())
		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
	})

	t.Run("block hash", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)

		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
	})

	t.Run("block hash - block 1", func(t *testing.T) {
		hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
		hashMerkleRoot, _ := chainhash.NewHashFromStr("6a6c0ec8d4adfe242b17153b4f2723b0cb6f783b1ca0f1e17cbdaf699a813316")
		nBits, _ := NewNBitFromString("207fffff")
		blockHeader := &BlockHeader{
			Version:        0x20000000,
			HashPrevBlock:  hashPrevBlock,
			HashMerkleRoot: hashMerkleRoot,
			Timestamp:      1729251723,
			Bits:           *nBits,
			Nonce:          4,
		}

		assert.Equal(t, "4c74e0128fef1a01469380c05b215afaf4cfe51183461f4a7996a84295b6925a", blockHeader.Hash().String())
	})
}

func Test_ToWireBlockHeader(t *testing.T) {
	t.Run("block 1", func(t *testing.T) {
		blockHeaderBytes, _ := hex.DecodeString(block1Header)
		blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
		require.NoError(t, err)

		// reader to bytes
		reader := bytes.NewReader(blockHeaderBytes)
		w := wire.BlockHeader{}
		err = w.Deserialize(reader)
		require.NoError(t, err)

		wireBlockHeader := blockHeader.ToWireBlockHeader()
		assert.Equal(t, blockHeader.Hash().String(), wireBlockHeader.BlockHash().String())
	})
}

func TestGetBlockJson(t *testing.T) {
	var blockData map[string]interface{}

	err := json.Unmarshal([]byte(getBlockJSON), &blockData)
	require.NoError(t, err)

	header, err := NewBlockHeaderFromJSON(getBlockJSON)
	if err != nil {
		t.Fatal(err)
	}

	ok, hash, err := header.HasMetTargetDifficulty()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, blockData["hash"], hash.String())

	assert.Equal(t, blockBytes[:80], header.Bytes())
}

func TestGetMiningCandidateJson(t *testing.T) {
	var miningCandidateData map[string]interface{}

	err := json.Unmarshal([]byte(getMiningCandidateJSON), &miningCandidateData)
	require.NoError(t, err)

	var blockData map[string]interface{}

	err = json.Unmarshal([]byte(getBlockJSON), &blockData)
	require.NoError(t, err)

	coinbaseTx, err := bt.NewTxFromString(miningCandidateData["coinbase"].(string))
	require.NoError(t, err)

	header, err := NewBlockHeaderFromJSON(getMiningCandidateJSON, coinbaseTx)
	if err != nil {
		t.Fatal(err)
	}

	header.Nonce = 1 // set nonce to 1 to make it valid

	// t.Logf("%+x\n", header.Bytes())

	ok, hash, err := header.HasMetTargetDifficulty()
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, blockData["hash"], hash.String())
	// t.Log(ok, hash)

	assert.Equal(t, blockBytes[:80], header.Bytes())
}

func TestSubmitMiningSolution(t *testing.T) {
	var miningSolution bsvjson.MiningSolution

	err := json.Unmarshal([]byte(submitMiningSolutionJSON), &miningSolution)
	require.NoError(t, err)

	assert.Equal(t, "b64176f4aada053a8d22b7d2755bfcf47793dc097dd87f2a69e6c6fb98cbf2a1", miningSolution.ID)
	assert.Equal(t, uint32(1731944075), *miningSolution.Time)
	assert.Equal(t, uint32(1), miningSolution.Nonce)
	assert.Equal(t, uint32(536870912), *miningSolution.Version)

	coinbase, err := bt.NewTxFromString(miningSolution.Coinbase)
	require.NoError(t, err)

	ms := &MiningSolution{
		Id:       []byte(miningSolution.ID),
		Coinbase: coinbase.Bytes(),
		Time:     miningSolution.Time,
		Nonce:    miningSolution.Nonce,
		Version:  miningSolution.Version,
	}

	assert.Equal(t, ms.Id, []byte(miningSolution.ID))
	assert.Equal(t, ms.Coinbase, coinbase.Bytes())
	assert.Equal(t, ms.Time, miningSolution.Time)
	assert.Equal(t, ms.Nonce, miningSolution.Nonce)
	assert.Equal(t, ms.Version, miningSolution.Version)
}

func TestBuildMerkleRootHashFromProofs(t *testing.T) {
	var data map[string]interface{}

	err := json.Unmarshal([]byte(getMiningCandidateJSON), &data)
	require.NoError(t, err)

	var miningSolution bsvjson.MiningSolution

	err = json.Unmarshal([]byte(submitMiningSolutionJSON), &miningSolution)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(miningSolution.Coinbase)
	require.NoError(t, err)

	merkleRoot, err := buildMerkleRootHashFromProofs(data, coinbase)
	require.NoError(t, err)

	mr := (*chainhash.Hash)(merkleRoot)

	assert.Equal(t, "69813d58079d5d2924cf62b9f183bc058c04a98e35e67060dcfb71ad5435cb8a", mr.String())
}

func TestCreateCoinbaseTransaction(t *testing.T) {
	coinbaseTx, err := createCoinbaseTx()
	require.NoError(t, err)

	assert.True(t, coinbaseTx.IsCoinbase())

	assert.Len(t, coinbaseTx.Inputs, 1)
	assert.Len(t, coinbaseTx.Outputs, 1)
}

func createCoinbaseTx() (*bt.Tx, error) {
	coinbaseTx := bt.NewTx()

	height := uint32(100)

	blockHeightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockHeightBytes, height) // Block height

	arbitraryData := []byte{}
	arbitraryData = append(arbitraryData, 0x03)
	arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
	arbitraryData = append(arbitraryData, []byte("/Test miner/")...)

	err := coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0)
	if err != nil {
		return nil, err
	}

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(arbitraryData)
	coinbaseTx.Inputs[0].SequenceNumber = 0

	err = coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 12200)
	if err != nil {
		return nil, err
	}

	return coinbaseTx, nil
}

func TestChainhashAssumptions(t *testing.T) {
	s := "0000000000000000000000000000000000000000000000000000000000000001"

	h1, err := chainhash.NewHashFromStr(s)
	require.NoError(t, err)

	b, err := util.DecodeAndReverseHashString(s)
	require.NoError(t, err)

	h2 := chainhash.Hash(b)

	h3, err := chainhash.NewHash(b[:])
	require.NoError(t, err)

	h4 := (*chainhash.Hash)(b[:])

	t.Logf("Hash: %s", h1.String())
	t.Logf("Hash: %s", h2.String())
	t.Logf("Hash: %s", h3.String())
	t.Logf("Hash: %s", h4.String())
}

func TestBlockWork(t *testing.T) {
	p, err := chainhash.NewHashFromStr("1a270fe33eab79fef413754296ae329b4fd3979feb8b8657597c54371ae524a3")
	require.NoError(t, err)

	mr, err := chainhash.NewHashFromStr("69813d58079d5d2924cf62b9f183bc058c04a98e35e67060dcfb71ad5435cb8a")
	require.NoError(t, err)

	b, err := NewNBitFromString("207fffff")
	require.NoError(t, err)

	bh := &BlockHeader{
		Version:        0x20000000,
		HashPrevBlock:  p,
		HashMerkleRoot: mr,
		Timestamp:      1731944075,
		Bits:           *b,
		Nonce:          1,
	}

	ok, hash, err := bh.HasMetTargetDifficulty()
	require.NoError(t, err)

	assert.True(t, ok)
	assert.Equal(t, "611fd97881064670555ac01db182c46134e770aa47d1a794b7df2767e42f3f89", hash.String())
}
