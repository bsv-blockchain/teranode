package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/util"
)

const dateFormat = "2006-01-02T15:04:05.000Z" // ISO 8601

func (mc *MiningCandidate) Stringify(long bool) string {
	var sb strings.Builder

	if !long {
		sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)", math.Pow(2, float64(len(mc.MerkleProof)))))
	} else {
		sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)\n\t", math.Pow(2, float64(len(mc.MerkleProof)))))
		sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", util.ReverseAndHexEncodeSlice(mc.Id)))
		sb.WriteString(fmt.Sprintf("Previous hash:  %s\n\t", util.ReverseAndHexEncodeSlice(mc.PreviousHash)))
		sb.WriteString(fmt.Sprintf("Coinbase value: %d\n\t", mc.CoinbaseValue))
		sb.WriteString(fmt.Sprintf("Version:        %d\n\t", mc.Version))
		sb.WriteString(fmt.Sprintf("nBits:          %s\n\t", util.ReverseAndHexEncodeSlice(mc.NBits)))
		sb.WriteString(fmt.Sprintf("Time:           %d\n\t", mc.Time))
		sb.WriteString(fmt.Sprintf("Height:         %d\n", mc.Height))
	}
	// sb.WriteString("Merkle Proof:\n")
	// for _, mp := range mc.MerkleProof {
	// 	sb.WriteString("\t")
	// 	sb.WriteString(hex.EncodeToString(mp))
	// 	sb.WriteString("\n")
	// }
	return sb.String()
}

func (ms *MiningSolution) Stringify(long bool) string {
	var sb strings.Builder

	if !long {
		sb.WriteString("Mining Solution for job ")
		sb.WriteString(util.ReverseAndHexEncodeSlice(ms.Id))
	} else {
		sb.WriteString("Mining Solution\n\t")
		sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", util.ReverseAndHexEncodeSlice(ms.Id)))
		sb.WriteString(fmt.Sprintf("Nonce:          %d\n\t", ms.Nonce))
		if ms.Time != nil {
			sb.WriteString(fmt.Sprintf("Time:           %d\n\t", *ms.Time))
		} else {
			sb.WriteString("Time:           <nil>\n\t")
		}
		if ms.Version != nil {
			sb.WriteString(fmt.Sprintf("Version:        %d\n\t", *ms.Version))
		} else {
			sb.WriteString("Version:        <nil>\n\t")
		}
		sb.WriteString(fmt.Sprintf("CoinbaseTX:     %x\n\n", ms.Coinbase))
	}

	return sb.String()
}

func (n *Notification) Stringify() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Type: %s, Hash: %s", n.Type, n.Hash.String()))

	return sb.String()
}

// Create custom JSON marshal and unmarshal receivers for BlockInfo
// so that we can return the JSON in the format we want.
func (bi *BlockInfo) MarshalJSON() ([]byte, error) {
	header, err := NewBlockHeaderFromBytes(bi.BlockHeader)
	if err != nil {
		return nil, err
	}

	hash := header.Hash()

	timestamp := time.Unix(int64(header.Timestamp), 0).UTC()

	type aliasResponse struct {
		Height            uint32 `json:"height"`
		Hash              string `json:"hash"`
		PreviousBlockHash string `json:"previousblockhash"`
		CoinbaseValue     uint64 `json:"coinbaseValue"`
		Timestamp         string `json:"timestamp"`
		TransactionCount  uint64 `json:"transactionCount"`
		Size              uint64 `json:"size"`
		Miner             string `json:"miner"`
	}

	a := aliasResponse{
		Height:            bi.Height,
		Hash:              hash.String(),
		PreviousBlockHash: header.HashPrevBlock.String(),
		CoinbaseValue:     bi.CoinbaseValue,
		Timestamp:         timestamp.Format(dateFormat),
		TransactionCount:  bi.TransactionCount,
		Size:              bi.Size,
		Miner:             bi.Miner,
	}

	return json.Marshal(a)
}
