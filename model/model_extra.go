package model

import (
	"fmt"
	"math"
	"strings"

	"github.com/ordishs/go-utils"
)

func (mc *MiningCandidate) Stringify() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)\n\t", math.Pow(2, float64(len(mc.MerkleProof)))))
	sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", utils.ReverseAndHexEncodeSlice(mc.Id)))
	sb.WriteString(fmt.Sprintf("Previous hash:  %s\n\t", utils.ReverseAndHexEncodeSlice(mc.PreviousHash)))
	sb.WriteString(fmt.Sprintf("Coinbase value: %d\n\t", mc.CoinbaseValue))
	sb.WriteString(fmt.Sprintf("Version:        %d\n\t", mc.Version))
	sb.WriteString(fmt.Sprintf("nBits:          %s\n\t", utils.ReverseAndHexEncodeSlice(mc.NBits)))
	sb.WriteString(fmt.Sprintf("Time:           %d\n\t", mc.Time))
	sb.WriteString(fmt.Sprintf("Height:         %d\n\n", mc.Height))
	// sb.WriteString("Merkle Proof:\n")
	// for _, mp := range mc.MerkleProof {
	// 	sb.WriteString("\t")
	// 	sb.WriteString(hex.EncodeToString(mp))
	// 	sb.WriteString("\n")
	// }

	return sb.String()
}

func (ms *MiningSolution) Stringify() string {
	var sb strings.Builder

	sb.WriteString("Mining Solution\n\t")
	sb.WriteString(fmt.Sprintf("Job ID:         %s\n\t", utils.ReverseAndHexEncodeSlice(ms.Id)))
	sb.WriteString(fmt.Sprintf("Nonce:          %d\n\t", ms.Nonce))
	sb.WriteString(fmt.Sprintf("Time:           %d\n\t", ms.Time))
	sb.WriteString(fmt.Sprintf("Version:        %d\n\t", ms.Version))
	sb.WriteString(fmt.Sprintf("CoinbaseTX:     %x\n\n", ms.Coinbase))

	return sb.String()
}
