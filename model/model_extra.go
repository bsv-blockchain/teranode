package model

import (
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/ordishs/go-utils"
)

func (mc *MiningCandidate) Stringify() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("Mining Candidate (%.0f transactions)\n\t", math.Pow(2, float64(len(mc.MerkleProof)))))

	sb.WriteString(hex.EncodeToString(mc.Id))
	sb.WriteString("\n\t")

	sb.WriteString(utils.ReverseAndHexEncodeSlice(mc.PreviousHash))
	sb.WriteString("\n\t")

	sb.WriteString(fmt.Sprintf("Coinbase value: %d\n\t", mc.CoinbaseValue))

	sb.WriteString(fmt.Sprintf("Version:        %d\n\t", mc.Version))

	sb.WriteString(hex.EncodeToString(mc.NBits))
	sb.WriteString("\n\t")

	sb.WriteString(fmt.Sprintf("Time:           %d\n\t", mc.Time))
	sb.WriteString(fmt.Sprintf("Height:         %d\n", mc.Height))
	// sb.WriteString("Merkle Proof:\n")
	// for _, mp := range mc.MerkleProof {
	// 	sb.WriteString("\t")
	// 	sb.WriteString(hex.EncodeToString(mp))
	// 	sb.WriteString("\n")
	// }

	return sb.String()
}
