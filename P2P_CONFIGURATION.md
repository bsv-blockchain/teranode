# P2P Configuration Guide for Teranode

This document describes the P2P configuration options available in Teranode after integrating the enhanced go-p2p library.

## Default Configuration Changes

**IMPORTANT**: As of this update, many P2P features are now **enabled by default** to provide better connectivity out of the box. This is a change from the previous version where all features defaulted to disabled.

### Features Enabled by Default
- ✅ **NAT Service** - Essential for address discovery
- ✅ **Hole Punching** - Critical for NAT traversal
- ✅ **Relay** - Fallback for difficult NAT scenarios
- ✅ **NAT Port Map** - Automatic port forwarding via UPnP
- ✅ **AutoNAT v2** - Improved address discovery
- ✅ **Connection Manager** - Prevents resource exhaustion
- ✅ **Peer Cache** - Faster network recovery after restarts

### Features Disabled by Default
- ❌ **Relay Service** - Only for well-resourced nodes willing to help others
- ❌ **Connection Gater** - Only needed for strict connection control

### Rationale for Default Changes
The previous configuration with all features disabled caused significant connectivity issues:
- Nodes behind NAT couldn't be reached
- No automatic port forwarding
- No connection limits leading to resource exhaustion
- Slow network recovery after restarts

The new defaults provide a much better out-of-the-box experience while maintaining reasonable resource usage.

## Enhanced NAT Traversal Features

### AutoNAT v2
- **Environment Variable**: `p2p_enable_autonat_v2`
- **Default**: `true` ✅
- **Description**: Enables AutoNAT v2 for improved automatic address discovery. This provides better NAT detection and more reliable address discovery compared to the original AutoNAT.
- **When to disable**: Only if experiencing issues with address detection or in controlled environments

### Force Reachability
- **Environment Variable**: `p2p_force_reachability`
- **Default**: `""` (auto-detect)
- **Options**: `"public"`, `"private"`, or `""` (auto-detect)
- **Description**: Forces the node to advertise itself as publicly reachable or private. Useful for bootstrap nodes that should always be considered public.
- **Recommended**: Set to `"public"` for bootstrap nodes

### Relay Service
- **Environment Variable**: `p2p_enable_relay_service`
- **Default**: `false`
- **Description**: When enabled (along with `p2p_enable_relay`), allows this node to act as a relay for other nodes that cannot establish direct connections.
- **Recommended**: Enable on well-connected nodes with good bandwidth

## Connection Management

### Connection Manager
- **Environment Variable**: `p2p_enable_conn_manager`
- **Default**: `true` ✅
- **Description**: Enables automatic connection management with high/low water marks to prevent resource exhaustion.

#### Connection Water Marks
- **Low Water**: `p2p_conn_low_water` (default: 200) - Minimum number of connections to maintain
- **High Water**: `p2p_conn_high_water` (default: 400) - Maximum connections before pruning begins
- **Grace Period**: `p2p_conn_grace_period` (default: 60s) - Time before new connections can be pruned

### Connection Gater
- **Environment Variable**: `p2p_enable_conn_gater`
- **Default**: `false`
- **Description**: Enables fine-grained connection control including per-peer connection limits and subnet blocking.

#### Max Connections Per Peer
- **Environment Variable**: `p2p_max_conns_per_peer`
- **Default**: `3`
- **Description**: Maximum number of simultaneous connections allowed from a single peer.

## Peer Persistence

### Enable Peer Cache
- **Environment Variable**: `p2p_enable_peer_cache`
- **Default**: `true` ✅
- **Description**: Enables caching of successful peer connections to disk for faster reconnection after restarts.

### Cache Configuration
- **Cache Directory**: `p2p_peer_cache_dir` (default: current working directory, file is always `teranode_peers.json`)
- **Max Cached Peers**: `p2p_max_cached_peers` (default: 100)
- **Cache TTL**: `p2p_peer_cache_ttl` (default: 30 days)

## Example Configurations

### Bootstrap Node Configuration
```bash
# Most features are already enabled by default
# Bootstrap-specific settings:
export p2p_force_reachability=public    # Force public reachability
export p2p_enable_relay_service=true    # Help other nodes connect

# Adjust connection limits for high traffic
export p2p_conn_low_water=500
export p2p_conn_high_water=1000
export p2p_conn_grace_period=120s
```

### Regular Node Behind NAT
```bash
# Most features work well with defaults
# Optional: Adjust connection limits if needed
export p2p_conn_low_water=100
export p2p_conn_high_water=200

# Optional: Reduce peer cache for smaller footprint
export p2p_max_cached_peers=50
```

### Resource-Constrained Node
```bash
# Disable some features to reduce resource usage
export p2p_enable_relay=false           # Don't act as relay
export p2p_enable_nat_port_map=false    # Skip UPnP if not needed

# Strict connection limits
export p2p_conn_low_water=20
export p2p_conn_high_water=50
export p2p_enable_conn_gater=true       # Enable strict control
export p2p_max_conns_per_peer=1

# Smaller peer cache
export p2p_max_cached_peers=20
```

### High-Security Environment
```bash
# Disable automatic features
export p2p_enable_nat_service=false
export p2p_enable_nat_port_map=false
export p2p_enable_autonat_v2=false
export p2p_enable_relay=false

# Strict connection control
export p2p_enable_conn_gater=true
export p2p_max_conns_per_peer=1
export p2p_conn_high_water=50

# Disable peer persistence
export p2p_enable_peer_cache=false
```

## Migration from Previous Configuration

The existing NAT configuration options remain unchanged:
- `p2p_enable_nat_service` - Basic NAT service
- `p2p_enable_nat_port_map` - UPnP/NAT-PMP port mapping
- `p2p_enable_hole_punching` - DCUtR hole punching
- `p2p_enable_relay` - Circuit relay v2

The new features are additive and can be enabled alongside existing configuration.

## Troubleshooting

### Node Not Reachable
1. Enable AutoNAT v2: `p2p_enable_autonat_v2=true`
2. Ensure NAT features are enabled
3. Check firewall rules
4. Consider setting explicit advertise addresses

### Too Many Connections
1. Enable connection manager: `p2p_enable_conn_manager=true`
2. Adjust water marks to appropriate levels
3. Enable connection gater for per-peer limits

### Slow Peer Discovery After Restart
1. Enable peer cache: `p2p_enable_peer_cache=true`
2. Ensure cache file location is writable
3. Increase `p2p_max_cached_peers` if needed

## Performance Considerations

- **Connection Manager**: Small overhead for tracking connections, but prevents resource exhaustion
- **Connection Gater**: Minimal overhead, useful for preventing abuse
- **Peer Cache**: Small disk I/O on startup/shutdown, significantly improves reconnection time
- **AutoNAT v2**: Slightly more network overhead than v1, but much better accuracy
- **Relay Service**: Can consume significant bandwidth if many peers use your node as relay

## Security Notes

- Setting `ForceReachability` to `public` on a private node may cause connection issues
- Connection gater helps prevent resource exhaustion attacks
- Peer cache files contain peer IDs and addresses - ensure proper file permissions
- Relay service should only be enabled on trusted, well-resourced nodes