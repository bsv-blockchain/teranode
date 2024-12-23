package blockassembly_api

import (
	"github.com/bitcoin-sv/teranode/model"
	"github.com/loopholelabs/polyglot"
)

type ModelMiningCandidate struct {
	model.MiningCandidate
	error error
}

func NewModelMiningCandidate() *ModelMiningCandidate {
	return &ModelMiningCandidate{}
}

func (m *ModelMiningCandidate) Decode(b polyglot.Buffer) error {
	// TODO: Implement
	return nil
}

func (m *ModelMiningCandidate) Encode(b *polyglot.Buffer) {
	// TODO: Implement
}

func (m *ModelMiningCandidate) Error(b *polyglot.Buffer, err error) {
	m.error = err
}
