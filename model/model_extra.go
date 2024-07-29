package model

import (
	"encoding/json"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"math"
	"strings"
	"time"

	"github.com/ordishs/go-utils"
)

const dateFormat = "2006-01-02T15:04:05.000Z" // ISO 8601

func (mc *MiningCandidate) Stringify(long bool) string {
	var sb strings.Builder

	if !long {
		sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)", math.Pow(2, float64(len(mc.MerkleProof)))))

	} else {
		sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)\n\t", math.Pow(2, float64(len(mc.MerkleProof)))))
		sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", utils.ReverseAndHexEncodeSlice(mc.Id)))
		sb.WriteString(fmt.Sprintf("Previous hash:  %s\n\t", utils.ReverseAndHexEncodeSlice(mc.PreviousHash)))
		sb.WriteString(fmt.Sprintf("Coinbase value: %d\n\t", mc.CoinbaseValue))
		sb.WriteString(fmt.Sprintf("Version:        %d\n\t", mc.Version))
		sb.WriteString(fmt.Sprintf("nBits:          %s\n\t", utils.ReverseAndHexEncodeSlice(mc.NBits)))
		sb.WriteString(fmt.Sprintf("Time:           %d\n\t", mc.Time))
		sb.WriteString(fmt.Sprintf("Height:         %d\n", mc.Height))
		// sb.WriteString("Merkle Proof:\n")
		// for _, mp := range mc.MerkleProof {
		// 	sb.WriteString("\t")
		// 	sb.WriteString(hex.EncodeToString(mp))
		// 	sb.WriteString("\n")
		// }
	}
	return sb.String()
}

func (ms *MiningSolution) Stringify(long bool) string {
	var sb strings.Builder

	if !long {
		sb.WriteString("Mining Solution for job ")
		sb.WriteString(utils.ReverseAndHexEncodeSlice(ms.Id))

	} else {
		sb.WriteString("Mining Solution\n\t")
		sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", utils.ReverseAndHexEncodeSlice(ms.Id)))
		sb.WriteString(fmt.Sprintf("Nonce:          %d\n\t", ms.Nonce))
		sb.WriteString(fmt.Sprintf("Time:           %d\n\t", ms.Time))
		sb.WriteString(fmt.Sprintf("Version:        %d\n\t", ms.Version))
		sb.WriteString(fmt.Sprintf("CoinbaseTX:     %x\n\n", ms.Coinbase))
	}

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

	miner, err := escapeJSON(bi.Miner)
	if err != nil {
		return nil, errors.NewProcessingError("could not parse miner '%s'", bi.Miner, err)
	}

	return []byte(fmt.Sprintf(`
	{
		"height": %d,
		"hash": "%s",
		"previousblockhash": "%s",
		"coinbaseValue": %d,
		"timestamp": "%s",
		"transactionCount": %d,
		"size": %d,
		"miner": "%s"
	}`,
		bi.Height,
		hash.String(),
		header.HashPrevBlock.String(),
		bi.CoinbaseValue,
		timestamp.Format(dateFormat),
		bi.TransactionCount,
		bi.Size,
		miner)), nil
}

func escapeJSON(input string) (string, error) {
	// Use json.Marshal to escape the input string.
	escapedJSON, err := json.Marshal(input)
	if err != nil {
		return "", err
	}

	// Convert the JSON bytes to a string, removing the surrounding double quotes.
	escapedString := string(escapedJSON[1 : len(escapedJSON)-1])

	// Return the escaped string.
	return escapedString, nil
}
