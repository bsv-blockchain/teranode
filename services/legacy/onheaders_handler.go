package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

// onHeaders is invoked when a peer receives a headers bitcoin message.
func (pm *PeerManager) onHeaders() func(p *peer.Peer, msg *wire.MsgHeaders) {
	return func(p *peer.Peer, msg *wire.MsgHeaders) {
		pm.logger.Infof("Received %d headers\n", len(msg.Headers))

		err := verifyHeadersChain(msg.Headers, pm.lastHash)
		if err != nil {
			pm.logger.Fatalf("Header chain verification failed: %v", err)
		}
		pm.logger.Infof("Header chain is valid")

		// s.wg.Add(len(msg.Headers))

		// Now get each block in turn and process it
		go func() {
			for _, header := range msg.Headers {
				pm.cond.L.Lock()

				blockHash := header.BlockHash()
				getDataMsg := wire.NewMsgGetData()
				_ = getDataMsg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &blockHash))

				pm.logger.Infof("Requesting block %s\n", blockHash)

				p.QueueMessage(getDataMsg, nil)
				pm.cond.Wait()

				pm.cond.L.Unlock()
			}

			// s.wg.Wait()

			// TODO this should be moved into the peer manager
			//
			//
			h := msg.Headers[len(msg.Headers)-1].BlockHash()
			pm.lastHash = &h

			msgGetHeaders := wire.NewMsgGetHeaders()
			// TODO: Bitcoin is expecting the last 12 hashes we've got, but for now we only send our last hash
			// https://wiki.bitcoinsv.io/index.php/Peer-To-Peer_Protocol#getheaders
			_ = msgGetHeaders.AddBlockLocatorHash(pm.lastHash)
			msgGetHeaders.HashStop = chainhash.Hash{}

			// Send the getheaders message
			pm.logger.Infof("Requesting headers starting from %s\n", pm.lastHash)

			p.QueueMessage(msgGetHeaders, nil)
			//
			//
			// end
		}()
	}
}
