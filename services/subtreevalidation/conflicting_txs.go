package subtreevalidation

import (
	"bytes"
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"google.golang.org/protobuf/proto"
)

// serializeConflictingTxs encodes the txs kept aside from a peer subtree, in subtree
// order, the way subtreeData encodes transactions.
func serializeConflictingTxs(txs []*bt.Tx) []byte {
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

// readConflictingTxs decodes a FileTypeSubtreeConflicting file.
func readConflictingTxs(b []byte) ([]*bt.Tx, error) {
	reader := bytes.NewReader(b)
	txs := make([]*bt.Tx, 0, 8)

	// bytes remain whenever a read starts, so any error, EOF included, means a truncated file
	for reader.Len() > 0 {
		tx := &bt.Tx{}
		if _, err := tx.ReadFrom(reader); err != nil {
			return nil, errors.NewProcessingError("[readConflictingTxs] failed to read transaction", err)
		}

		txs = append(txs, tx)
	}

	return txs, nil
}

// keptAsideToPrewarm returns the txs kept aside next to subtree when its subtree data is
// not stored, the only txs of it a block needs to prewarm; see subtreeConflictingTxs.
func (u *Server) keptAsideToPrewarm(ctx context.Context, subtree *subtreepkg.Subtree, subtreeDataExists bool) ([]*bt.Tx, bool, error) {
	if subtreeDataExists {
		return nil, false, nil
	}

	return u.subtreeConflictingTxs(ctx, subtree)
}

// readKeptAside reads the txs kept aside next to subtreeHash. found is false when there is
// no such file, or when it is unreadable: content that cannot be read only spares a peer
// request, so the caller then fetches the txs as before. A storage error halts.
func (u *Server) readKeptAside(ctx context.Context, subtreeHash chainhash.Hash) (txs []*bt.Tx, found bool, err error) {
	b, err := u.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeConflicting)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return nil, false, nil
		}

		return nil, false, errors.NewStorageError("[readKeptAside][%s] failed to read kept-aside conflicting txs", subtreeHash.String(), err)
	}

	txs, err = readConflictingTxs(b)
	if err != nil {
		u.logger.Warnf("[readKeptAside][%s] ignoring unreadable kept-aside conflicting txs: %v", subtreeHash.String(), err)
		return nil, false, nil
	}

	return txs, true, nil
}

// lookupConflictingTxs resolves missing txs from the txs kept aside next to the subtree,
// if any. A tx only matches the hash it hashes to, so the file cannot substitute one tx
// for another; every tx it supplies is still validated.
func (u *Server) lookupConflictingTxs(ctx context.Context, subtreeHash chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData) (found []missingTx, stillMissing []utxo.UnresolvedMetaData, err error) {
	txs, ok, err := u.readKeptAside(ctx, subtreeHash)
	if err != nil {
		return nil, nil, err
	}

	if !ok {
		return nil, missingTxHashes, nil
	}

	byHash := make(map[chainhash.Hash]*bt.Tx, len(txs))
	for _, tx := range txs {
		byHash[*tx.TxIDChainHash()] = tx
	}

	for _, h := range missingTxHashes {
		if tx, ok := byHash[h.Hash]; ok {
			found = append(found, missingTx{tx: tx, idx: h.Idx})
		} else {
			stillMissing = append(stillMissing, h)
		}
	}

	return found, stillMissing, nil
}

// subtreeConflictingTxs returns the txs kept aside next to subtree, if there are any and
// every one of them is a node of subtree. Otherwise ok is false and the caller loads the
// subtree's txs the usual way.
func (u *Server) subtreeConflictingTxs(ctx context.Context, subtree *subtreepkg.Subtree) (txs []*bt.Tx, ok bool, err error) {
	subtreeHash := *subtree.RootHash()

	txs, ok, err = u.readKeptAside(ctx, subtreeHash)
	if err != nil || !ok {
		return nil, false, err
	}

	pending := make(map[chainhash.Hash]struct{}, len(txs))
	for _, tx := range txs {
		pending[*tx.TxIDChainHash()] = struct{}{}
	}

	for _, node := range subtree.Nodes {
		delete(pending, node.Hash)
	}

	if len(pending) > 0 {
		u.logger.Warnf("[subtreeConflictingTxs][%s] ignoring kept-aside conflicting txs: %d are not in the subtree", subtreeHash.String(), len(pending))
		return nil, false, nil
	}

	return txs, true, nil
}

// incompleteTreeFromNodes builds a subtree of leafCount nodes from their hashes, node(i)
// returning the i-th, with the coinbase placeholder added as the coinbase node.
func incompleteTreeFromNodes(leafCount int, node func(i int) chainhash.Hash) (*subtreepkg.Subtree, error) {
	tree, err := subtreepkg.NewIncompleteTreeByLeafCount(leafCount)
	if err != nil {
		return nil, errors.NewProcessingError("failed to create subtree structure", err)
	}

	if err = addNodes(tree, leafCount, node); err != nil {
		return nil, errors.NewProcessingError("failed to add nodes to subtree", err)
	}

	return tree, nil
}

// addNodes adds leafCount node hashes to tree, node(i) returning the i-th. Whose fault a rejected
// list is depends on where it came from, so the caller's code on top decides it: a peer code
// outranks the processing error under it when a peer is charged.
func addNodes(tree *subtreepkg.Subtree, leafCount int, node func(i int) chainhash.Hash) error {
	for i := 0; i < leafCount; i++ {
		nodeHash := node(i)

		if nodeHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			if err := tree.AddCoinbaseNode(); err != nil {
				return errors.NewProcessingError("failed to add coinbase node to subtree", err)
			}
		} else if err := tree.AddNode(nodeHash, 0, 0); err != nil {
			return errors.NewProcessingError("failed to add node to subtree", err)
		}
	}

	return nil
}

// subtreeToCheckFromNodes builds the FileTypeSubtreeToCheck form of a subtree from its
// leafCount node hashes, node(i) returning the i-th, and checks it against the subtree's root.
// The node hashes are a peer's, so a list the tree rejects (the coinbase placeholder anywhere
// but first) or one that does not hash to the root is the peer's fault.
func subtreeToCheckFromNodes(subtreeHash chainhash.Hash, leafCount int, node func(i int) chainhash.Hash) (*subtreepkg.Subtree, []byte, error) {
	subtreeToCheck, err := subtreepkg.NewIncompleteTreeByLeafCount(leafCount)
	if err != nil {
		return nil, nil, errors.NewProcessingError("[subtreeToCheckFromNodes][%s] failed to create subtree structure", subtreeHash.String(), err)
	}

	if err = addNodes(subtreeToCheck, leafCount, node); err != nil {
		return nil, nil, errors.NewNetworkInvalidResponseError("[subtreeToCheckFromNodes][%s] failed to build subtree", subtreeHash.String(), err)
	}

	if !subtreeHash.Equal(*subtreeToCheck.RootHash()) {
		return nil, nil, errors.NewNetworkInvalidResponseError("[subtreeToCheckFromNodes][%s] subtree root hash mismatch: %s", subtreeHash.String(), subtreeToCheck.RootHash().String())
	}

	subtreeBytes, err := subtreeToCheck.Serialize()
	if err != nil {
		return nil, nil, errors.NewProcessingError("[subtreeToCheckFromNodes][%s] failed to serialize subtree", subtreeHash.String(), err)
	}

	return subtreeToCheck, subtreeBytes, nil
}

// storeSubtreeWithConflictingTxs stores a peer subtree that has txs kept aside. It is
// never stored as FileTypeSubtree: readers trust that file to have every tx in the UTXO
// store. The kept-aside txs are stored first and the subtree as FileTypeSubtreeToCheck
// second, so a subtree to check from this path always has its kept-aside txs next to it.
// A kept-aside file left by a pass that stopped before the subtree to check is replaced:
// this pass may have kept a different set aside. The block that references the subtree
// validates it, kept-aside txs included.
//
// Only the kept-aside txs the peer sent are written. Those this node resolved from its own
// caches are resolved again when the block needs them; writing them would let a peer fill
// the subtree store with txs it never sent, at 32 bytes a tx. The cost: if the caches have
// evicted such a tx by then and the peer cannot serve it, the block's subtree fails and is
// retried. Over the per-subtree cap the subtree fails, as it did before txs were kept aside.
func (u *Server) storeSubtreeWithConflictingTxs(ctx context.Context, subtreeHash chainhash.Hash, txHashes []chainhash.Hash, keptAside []missingTx) error {
	_, subtreeBytes, err := subtreeToCheckFromNodes(subtreeHash, len(txHashes), func(i int) chainhash.Hash { return txHashes[i] })
	if err != nil {
		return err
	}

	sent := make([]*bt.Tx, 0, len(keptAside))
	all := make([]*bt.Tx, 0, len(keptAside))
	sentBytes := 0

	for _, mTx := range keptAside {
		all = append(all, mTx.tx)

		if !mTx.cached {
			sent = append(sent, mTx.tx)
			sentBytes += mTx.tx.Size()
		}
	}

	if maxBytes := u.settings.SubtreeValidation.KeptAsideMaxBytesPerSubtree; sentBytes > maxBytes {
		return errors.NewProcessingError("[storeSubtreeWithConflictingTxs][%s] %d bytes of conflicting txs kept aside, over the %d byte limit", subtreeHash.String(), sentBytes, maxBytes)
	}

	// A kept-aside tx is not fully validated, so the previous-output data a peer attached to
	// it in extended format was never checked; it is dropped before the tx is kept anywhere.
	// The block path validates the plain tx, reading its parents from the UTXO store.
	sent, err = withoutExtendedData(sent)
	if err != nil {
		return errors.NewProcessingError("[storeSubtreeWithConflictingTxs][%s] failed to drop extended data of kept-aside txs", subtreeHash.String(), err)
	}

	all, err = withoutExtendedData(all)
	if err != nil {
		return errors.NewProcessingError("[storeSubtreeWithConflictingTxs][%s] failed to drop extended data of kept-aside txs", subtreeHash.String(), err)
	}

	dah := u.utxoStore.GetBlockHeight() + u.settings.GetSubtreeValidationBlockHeightRetention()

	if err = u.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeConflicting, serializeConflictingTxs(sent), options.WithDeleteAt(dah), options.WithAllowOverwrite(true)); err != nil {
		return errors.NewStorageError("[storeSubtreeWithConflictingTxs][%s] failed to store conflicting txs", subtreeHash.String(), err)
	}

	if err = u.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes, options.WithDeleteAt(dah)); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
		return errors.NewStorageError("[storeSubtreeWithConflictingTxs][%s] failed to store subtree to check", subtreeHash.String(), err)
	}

	u.shareConflictingTxs(all)

	prometheusSubtreeValidationConflictingKeptAside.Add(float64(len(keptAside)))

	u.logger.Infof("[storeSubtreeWithConflictingTxs][%s] kept %d conflicting txs aside until a block references the subtree, %d of them sent by the peer", subtreeHash.String(), len(keptAside), len(sent))

	return nil
}

// withoutExtendedData returns txs in plain format, without the previous output data an
// extended tx carries.
func withoutExtendedData(txs []*bt.Tx) ([]*bt.Tx, error) {
	plain := make([]*bt.Tx, len(txs))

	for i, tx := range txs {
		p, err := bt.NewTxFromBytes(tx.Bytes())
		if err != nil {
			return nil, err
		}

		plain[i] = p
	}

	return plain, nil
}

// cacheKeptAside caches a kept-aside tx on this pod, in plain format and under the size cap
// the consumers apply, unless the validator already reported it.
func (u *Server) cacheKeptAside(tx *bt.Tx) {
	hash := *tx.TxIDChainHash()

	if u.keptAsideTxCache == nil || u.policyRejectedTxCache.Conflicting(hash) {
		return
	}

	plain, err := bt.NewTxFromBytes(tx.Bytes())
	if err != nil || plain.Size() > maxCachedTxBytes {
		return
	}

	u.keptAsideTxCache.SetConflicting(hash, plain)
}

// shareConflictingTxs publishes kept-aside txs on the policy-rejected topic, flagged
// keptAside, for every other subtree validator, and caches them here. A tx the validator
// already reported is skipped. Mostly these are descendants; a loser whose validator
// publish this pod has not consumed yet goes out again. A consumer that already has the
// validator's message skips it, and one that gets it first counts the tx once when both
// arrive. A tx another subtree validator already published can go out again too; the
// consumer's insert is then a no-op. The publish is best-effort, like the validators': a
// dropped message only means a child in another subtree fails that subtree, as before.
func (u *Server) shareConflictingTxs(keptAside []*bt.Tx) {
	for _, tx := range keptAside {
		hash := *tx.TxIDChainHash()

		if u.policyRejectedTxCache.Conflicting(hash) {
			continue
		}

		// the cap the consumers apply, here too: one large tx would evict every other entry
		rawTx := tx.SerializeBytes()
		if len(rawTx) > maxCachedTxBytes {
			continue
		}

		if u.keptAsideTxCache != nil {
			u.keptAsideTxCache.SetConflicting(hash, tx)
		}

		if u.conflictingTxKafkaProducer == nil {
			continue
		}

		value, err := proto.Marshal(&kafkamessage.KafkaTxPolicyRejectedTopicMessage{
			TxHash:      hash.CloneBytes(),
			RawTx:       rawTx,
			Reason:      "kept aside: spends a conflicting tx",
			Conflicting: true,
			KeptAside:   true,
		})
		if err != nil {
			u.logger.Errorf("[shareConflictingTxs][%s] failed to marshal kept-aside conflicting tx: %v", hash.String(), err)
			continue
		}

		if !u.conflictingTxKafkaProducer.TryPublish(&kafka.Message{Key: hash.CloneBytes(), Value: value}) {
			u.logger.Debugf("[shareConflictingTxs][%s] dropped kept-aside conflicting tx: producer buffer full", hash.String())
		}
	}
}

// keptAsideSet tracks the txs of one peer subtree kept aside while its missing txs are
// blessed level by level, and decides which txs are kept aside.
type keptAsideSet struct {
	u           *Server
	txMetaSlice []metaSliceItem
	blockHeight uint32

	mu     sync.Mutex
	hashes map[chainhash.Hash]struct{}
}

// set keeps tx aside. It also makes the tx known on this pod at once, not when its subtree
// is stored, so a child in a sibling subtree validated at the same time is kept aside too.
// If the subtree then fails, the entry stays: a child of it elsewhere is kept aside instead
// of failing, which only moves its validation to the block, as for any kept-aside tx.
func (k *keptAsideSet) set(tx *bt.Tx, txIdx int) {
	k.mu.Lock()
	k.hashes[*tx.TxIDChainHash()] = struct{}{}
	k.mu.Unlock()

	k.txMetaSlice[txIdx] = metaSliceItem{keptAside: true}

	k.u.cacheKeptAside(tx)
}

// hasParentIn reports whether tx spends a tx kept aside in an earlier level of this subtree.
// Parents are always in an earlier level, so the set is complete for this tx's inputs, and
// such a parent is never in the UTXO store.
func (k *keptAsideSet) hasParentIn(tx *bt.Tx) bool {
	k.mu.Lock()
	defer k.mu.Unlock()

	for _, input := range tx.Inputs {
		if _, ok := k.hashes[*input.PreviousTxIDChainHash()]; ok {
			return true
		}
	}

	return false
}

// keepDescendant keeps aside a tx spending a kept-aside tx. It cannot be validated without
// its parent in the store, so it only gets the checks that need no parent; their failure
// is returned and the tx is not kept aside.
func (k *keptAsideSet) keepDescendant(tx *bt.Tx, txIdx int) error {
	if err := validator.CheckTransactionContextFree(tx, k.u.settings, k.blockHeight); err != nil {
		return err
	}

	k.set(tx, txIdx)

	return nil
}

// afterFailedBless keeps aside a tx whose bless failed with blessErr, when the validator
// rejected it as conflicting, or reported a missing parent that is known as conflicting.
// The validator marks a rejection conflicting only when every failed input is spent by a
// different tx, so a tx with an input that is invalid otherwise fails; the stores' per-input
// TX_CONFLICTING does not count. err is the error the bless now fails with, when the tx is
// not kept aside: a context-free failure replaces a missing parent. fatal is a store error,
// which halts the subtree.
func (k *keptAsideSet) afterFailedBless(ctx context.Context, tx *bt.Tx, txIdx int, blessErr error) (keptAside bool, err error, fatal error) {
	if errors.Is(blessErr, errors.ErrTxRejectedConflicting) {
		k.set(tx, txIdx)
		return true, nil, nil
	}

	if !errors.Is(blessErr, errors.ErrTxMissingParent) {
		return false, blessErr, nil
	}

	// as for a descendant: only the checks that need no parent, and those first, so a
	// malformed tx fails as invalid without a store read
	if ctxFreeErr := validator.CheckTransactionContextFree(tx, k.u.settings, k.blockHeight); ctxFreeErr != nil {
		return false, ctxFreeErr, nil
	}

	conflictingParent, parentErr := k.u.hasConflictingParent(ctx, tx)
	if parentErr != nil {
		return false, nil, parentErr
	}

	if !conflictingParent {
		return false, blessErr, nil
	}

	k.set(tx, txIdx)

	return true, nil, nil
}

// hasConflictingParent reports whether a missing parent of tx is known as conflicting. It
// is only asked once the validator found a parent missing. The caches are never cleared,
// so a parent counts only if it is also absent from the store: a parent that later won is
// in the store and decides. Each distinct parent is read once, straight from the store: a
// miss is the expected answer, not a cache miss to warn about.
func (u *Server) hasConflictingParent(ctx context.Context, tx *bt.Tx) (bool, error) {
	checked := make(map[chainhash.Hash]struct{}, len(tx.Inputs))

	for _, input := range tx.Inputs {
		parentHash := input.PreviousTxIDChainHash()

		if _, done := checked[*parentHash]; done {
			continue
		}

		checked[*parentHash] = struct{}{}

		if !u.knownConflicting(*parentHash) {
			continue
		}

		_, err := u.utxoStore.Get(ctx, parentHash, fields.Conflicting)
		if err == nil {
			continue
		}

		if errors.Is(err, errors.ErrTxNotFound) {
			return true, nil
		}

		return false, errors.NewStorageError("[validateSubtree][%s] failed to read parent %s", tx.TxIDChainHash().String(), parentHash.String(), err)
	}

	return false, nil
}
