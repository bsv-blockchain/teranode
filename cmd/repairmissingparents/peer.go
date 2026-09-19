package repairmissingparents

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// Peer is the read-only view of a healthy node's asset service that the repair needs.
type Peer interface {
	// GetTx returns the extended transaction bytes for hash, or errors.ErrNotFound when
	// the peer does not hold the transaction.
	GetTx(ctx context.Context, hash *chainhash.Hash) (*bt.Tx, error)
	// GetTxMeta returns the mined-state metadata for hash, or errors.ErrNotFound.
	GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*peerTxMeta, error)
	// GetUTXOs returns one entry per output of hash with its spent state, or errors.ErrNotFound.
	GetUTXOs(ctx context.Context, hash *chainhash.Hash) ([]peerUTXO, error)
}

// peerTxMeta is the subset of GET /api/v1/txmeta/:hash/json the repair reads.
// blockHashes is aligned with blockHeights and subtreeIdxs; an empty hash means the
// peer could not resolve that block ID.
type peerTxMeta struct {
	BlockHeights []uint32 `json:"blockHeights"`
	BlockHashes  []string `json:"blockHashes"`
	SubtreeIdxs  []int    `json:"subtreeIdxs"`
	IsCoinbase   bool     `json:"isCoinbase"`
}

// peerUTXO is one element of GET /api/v1/utxos/:hash/json. Status is the utxo.Status
// name ("OK", "SPENT", "NOT_FOUND", ...). SpendingData is present only when spent.
type peerUTXO struct {
	Vout         uint32              `json:"vout"`
	Status       string              `json:"status"`
	SpendingData *spend.SpendingData `json:"spendingData,omitempty"`
}

const statusSpent = "SPENT"

// HTTPPeer reads from an asset service over HTTP.
type HTTPPeer struct {
	baseURL string
	client  *http.Client
}

// NewHTTPPeer returns a Peer for the asset service at baseURL (scheme://host[:port],
// without the /api/v1 prefix). A nil client uses a 30-second-timeout default.
func NewHTTPPeer(baseURL string, client *http.Client) *HTTPPeer {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	return &HTTPPeer{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

func (p *HTTPPeer) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return nil, errors.NewProcessingError("build request %s", path, err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errors.NewServiceError("peer request %s", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, errors.NewServiceError("read peer response %s", path, err)
	}

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, errors.NewNotFoundError("peer has no record for %s", path)
	case resp.StatusCode != http.StatusOK:
		return nil, errors.NewServiceError("peer %s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return body, nil
}

func (p *HTTPPeer) GetTx(ctx context.Context, hash *chainhash.Hash) (*bt.Tx, error) {
	body, err := p.get(ctx, "/api/v1/tx/"+hash.String())
	if err != nil {
		return nil, err
	}

	tx, err := bt.NewTxFromBytes(body)
	if err != nil {
		return nil, errors.NewProcessingError("peer returned unparsable tx for %s", hash.String(), err)
	}

	if !tx.TxIDChainHash().IsEqual(hash) {
		return nil, errors.NewProcessingError("peer returned tx %s for requested %s", tx.TxID(), hash.String())
	}

	return tx, nil
}

func (p *HTTPPeer) GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*peerTxMeta, error) {
	body, err := p.get(ctx, fmt.Sprintf("/api/v1/txmeta/%s/json", hash.String()))
	if err != nil {
		return nil, err
	}

	m := &peerTxMeta{}
	if err := json.Unmarshal(body, m); err != nil {
		return nil, errors.NewProcessingError("peer returned unparsable txmeta for %s", hash.String(), err)
	}

	return m, nil
}

func (p *HTTPPeer) GetUTXOs(ctx context.Context, hash *chainhash.Hash) ([]peerUTXO, error) {
	body, err := p.get(ctx, fmt.Sprintf("/api/v1/utxos/%s/json", hash.String()))
	if err != nil {
		return nil, err
	}

	var items []peerUTXO
	if err := json.Unmarshal(body, &items); err != nil {
		return nil, errors.NewProcessingError("peer returned unparsable utxos for %s", hash.String(), err)
	}

	return items, nil
}
