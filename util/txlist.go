package util

import (
	"bytes"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
)

// SerializeTxList encodes txs back to back, the way subtreeData encodes transactions. A tx that
// carries previous-output data is written in extended format.
func SerializeTxList(txs []*bt.Tx) []byte {
	var buf bytes.Buffer

	for _, tx := range txs {
		if tx.IsExtended() {
			buf.Write(tx.ExtendedBytes())
		} else {
			buf.Write(tx.Bytes())
		}
	}

	return buf.Bytes()
}

// ReadTxList decodes the output of SerializeTxList. Bytes that do not decode to whole txs are an
// error.
func ReadTxList(b []byte) ([]*bt.Tx, error) {
	reader := bytes.NewReader(b)
	txs := make([]*bt.Tx, 0, 8)

	// bytes remain whenever a read starts, so any error, EOF included, means a truncated list
	for reader.Len() > 0 {
		tx := &bt.Tx{}
		if _, err := tx.ReadFrom(reader); err != nil {
			return nil, errors.NewProcessingError("[ReadTxList] failed to read transaction", err)
		}

		txs = append(txs, tx)
	}

	return txs, nil
}
