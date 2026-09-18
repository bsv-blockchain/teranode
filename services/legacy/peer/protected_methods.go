package peer

// ProtectedMethods is the full gRPC method paths of every state-mutating RPC
// on PeerService; the auth interceptor requires the admin API key for these.
// ClearBanned wipes the entire ban list in one call - a strict superset of
// UnbanPeer, which sits next to it here - so leaving it out would let an
// attacker unban everyone at once while still being unable to call UnbanPeer
// directly. Any new mutating RPC must be added here; the classification is
// enforced by TestProtectedMethodsCoverAllRPCs in services/legacy.
//
// This lives in the peer package, rather than services/legacy where it is
// enforced server-side, so that peer.Client can scope its own APIKeyMethods
// to the same set without an import cycle (services/legacy already imports
// this package).
var ProtectedMethods = map[string]bool{
	"/peer_api.PeerService/BanPeer":     true,
	"/peer_api.PeerService/UnbanPeer":   true,
	"/peer_api.PeerService/ClearBanned": true,
}
