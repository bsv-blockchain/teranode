package blockchain

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// This explicit inventory forces review when any RPC (including streams) is added.
// Runtime authorization defaults to required even before this inventory is updated.
func TestBlockchainAuthMethodCoverage(t *testing.T) {
	authenticated := map[string]bool{
		"/blockchain_api.BlockchainAPI/AddBlock":                             true,
		"/blockchain_api.BlockchainAPI/GetBlock":                             true,
		"/blockchain_api.BlockchainAPI/GetBlocks":                            true,
		"/blockchain_api.BlockchainAPI/GetBlockByHeight":                     true,
		"/blockchain_api.BlockchainAPI/GetBlockByID":                         true,
		"/blockchain_api.BlockchainAPI/GetNextBlockID":                       true,
		"/blockchain_api.BlockchainAPI/AssignBlockID":                        true,
		"/blockchain_api.BlockchainAPI/GetBlockStats":                        true,
		"/blockchain_api.BlockchainAPI/GetBlockGraphData":                    true,
		"/blockchain_api.BlockchainAPI/GetLastNBlocks":                       true,
		"/blockchain_api.BlockchainAPI/GetLastNInvalidBlocks":                true,
		"/blockchain_api.BlockchainAPI/GetSuitableBlock":                     true,
		"/blockchain_api.BlockchainAPI/GetHashOfAncestorBlock":               true,
		"/blockchain_api.BlockchainAPI/GetLatestBlockHeaderFromBlockLocator": true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersFromOldest":            true,
		"/blockchain_api.BlockchainAPI/GetNextWorkRequired":                  true,
		"/blockchain_api.BlockchainAPI/GetBlockExists":                       true,
		"/blockchain_api.BlockchainAPI/GetBlockHeaders":                      true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersToCommonAncestor":      true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersFromCommonAncestor":    true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersFromTill":              true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersFromHeight":            true,
		"/blockchain_api.BlockchainAPI/GetBlockHeadersByHeight":              true,
		"/blockchain_api.BlockchainAPI/GetMedianTimePastByHeights":           true,
		"/blockchain_api.BlockchainAPI/GetBlocksByHeight":                    true,
		"/blockchain_api.BlockchainAPI/FindBlocksContainingSubtree":          true,
		"/blockchain_api.BlockchainAPI/GetBlockHeaderIDs":                    true,
		"/blockchain_api.BlockchainAPI/GetBestBlockHeader":                   true,
		"/blockchain_api.BlockchainAPI/CheckBlockIsInCurrentChain":           true,
		"/blockchain_api.BlockchainAPI/CheckBlockIsAncestorOfBlock":          true,
		"/blockchain_api.BlockchainAPI/GetChainTips":                         true,
		"/blockchain_api.BlockchainAPI/GetBlockHeader":                       true,
		"/blockchain_api.BlockchainAPI/InvalidateBlock":                      true,
		"/blockchain_api.BlockchainAPI/RevalidateBlock":                      true,
		"/blockchain_api.BlockchainAPI/Subscribe":                            true,
		"/blockchain_api.BlockchainAPI/SendNotification":                     true,
		"/blockchain_api.BlockchainAPI/GetSubscribers":                       true,
		"/blockchain_api.BlockchainAPI/GetState":                             true,
		"/blockchain_api.BlockchainAPI/SetState":                             true,
		"/blockchain_api.BlockchainAPI/GetBlockIsMined":                      true,
		"/blockchain_api.BlockchainAPI/SetBlockMinedSet":                     true,
		"/blockchain_api.BlockchainAPI/ClearBlockMinedSet":                   true,
		"/blockchain_api.BlockchainAPI/GetBlocksMinedNotSet":                 true,
		"/blockchain_api.BlockchainAPI/SetBlockSubtreesSet":                  true,
		"/blockchain_api.BlockchainAPI/GetBlocksSubtreesNotSet":              true,
		"/blockchain_api.BlockchainAPI/SetBlockProcessedAt":                  true,
		"/blockchain_api.BlockchainAPI/SetBlockPersistedAt":                  true,
		"/blockchain_api.BlockchainAPI/GetBlocksNotPersisted":                true,
		"/blockchain_api.BlockchainAPI/SendFSMEvent":                         true,
		"/blockchain_api.BlockchainAPI/GetFSMCurrentState":                   true,
		"/blockchain_api.BlockchainAPI/WaitUntilFSMTransitionFromIdleState":  true,
		"/blockchain_api.BlockchainAPI/Run":                                  true,
		"/blockchain_api.BlockchainAPI/CatchUpBlocks":                        true,
		"/blockchain_api.BlockchainAPI/Idle":                                 true,
		"/blockchain_api.BlockchainAPI/ReportPeerFailure":                    true,
		"/blockchain_api.BlockchainAPI/GetBlockLocator":                      true,
		"/blockchain_api.BlockchainAPI/LocateBlockHeaders":                   true,
		"/blockchain_api.BlockchainAPI/GetBestHeightAndTime":                 true,
		"/blockchain_api.BlockchainAPI/ScheduleBlobDeletion":                 true,
		"/blockchain_api.BlockchainAPI/CancelBlobDeletion":                   true,
		"/blockchain_api.BlockchainAPI/ListScheduledDeletions":               true,
		"/blockchain_api.BlockchainAPI/GetPendingBlobDeletions":              true,
		"/blockchain_api.BlockchainAPI/RemoveBlobDeletion":                   true,
		"/blockchain_api.BlockchainAPI/IncrementBlobDeletionRetry":           true,
		"/blockchain_api.BlockchainAPI/CompleteBlobDeletions":                true,
		"/blockchain_api.BlockchainAPI/AcquireBlobDeletionBatch":             true,
		"/blockchain_api.BlockchainAPI/CompleteBlobDeletionBatch":            true,
		"/blockchain_api.PeerRegistryService/RegisterPeer":                   true,
		"/blockchain_api.PeerRegistryService/UpdatePeerMetrics":              true,
		"/blockchain_api.PeerRegistryService/RemovePeer":                     true,
		"/blockchain_api.PeerRegistryService/ListPeers":                      true,
		"/blockchain_api.PeerRegistryService/GetPeer":                        true,
		"/blockchain_api.PeerRegistryService/AddBanScore":                    true,
		"/blockchain_api.PeerRegistryService/IsPeerBanned":                   true,
		"/blockchain_api.PeerRegistryService/ListBannedPeers":                true,
		"/blockchain_api.PeerRegistryService/ClearBannedPeers":               true,
		"/blockchain_api.PeerRegistryService/UpdateConnectionState":          true,
		"/blockchain_api.PeerRegistryService/UpdateLastMessageTime":          true,
		"/blockchain_api.PeerRegistryService/UpdateStorage":                  true,
		"/blockchain_api.PeerRegistryService/RecordSyncAttempt":              true,
		"/blockchain_api.PeerRegistryService/ClearAllSyncAttempts":           true,
		"/blockchain_api.PeerRegistryService/RecordBlockReceived":            true,
		"/blockchain_api.PeerRegistryService/RecordSubtreeReceived":          true,
		"/blockchain_api.PeerRegistryService/RecordTransactionReceived":      true,
		"/blockchain_api.PeerRegistryService/RecordCatchupError":             true,
		"/blockchain_api.PeerRegistryService/RecordCatchupAttempt":           true,
		"/blockchain_api.PeerRegistryService/RecordCatchupSuccess":           true,
		"/blockchain_api.PeerRegistryService/RecordCatchupFailure":           true,
		"/blockchain_api.PeerRegistryService/ResetReputation":                true,
		"/blockchain_api.PeerRegistryService/ReconsiderBadPeers":             true,
		"/blockchain_api.PeerRegistryService/RecordValidatedPeerProgress":    true,
	}
	opts := (&Blockchain{settings: &settings.Settings{GRPCAdminAPIKey: blockchainAuthTestKey}}).grpcAuthOptions()
	require.True(t, opts.RequireAuthByDefault)
	require.Equal(t, map[string]bool{blockchain_api.BlockchainAPI_HealthGRPC_FullMethodName: true}, opts.PublicMethods)
	registered := map[string]bool{}
	services := blockchain_api.File_services_blockchain_blockchain_api_blockchain_api_proto.Services()
	for i := 0; i < services.Len(); i++ {
		svc := services.Get(i)
		for j := 0; j < svc.Methods().Len(); j++ {
			method := "/" + string(svc.FullName()) + "/" + string(svc.Methods().Get(j).Name())
			registered[method] = true
			require.NotEqual(t, authenticated[method], opts.PublicMethods[method], "%s must have exactly one classification", method)
		}
	}
	for method := range authenticated {
		require.True(t, registered[method], "stale method: %s", method)
	}
	for method := range opts.PublicMethods {
		require.True(t, registered[method], "stale exception: %s", method)
	}
}
