# P2P NAT Traversal and Address Filtering

## Problem

When a node is behind NAT with only private IP addresses (192.168.x.x, 10.x.x.x, 172.16.x.x), proper address discovery is critical:

1. The node only has private IP addresses on its network interfaces
2. go-p2p v1.1.18 aggressively filters private IPs when `advertiseAddresses` is empty
3. This filtering prevents libp2p's observed address mechanism from working
4. Nodes end up invisible to the network

## How libp2p Handles NAT

libp2p has several mechanisms for NAT traversal:

1. **Identify Protocol**: When a peer connects to you, it tells you what address it sees you coming from. This "observed address" is your public IP as seen from the outside.

2. **AutoNAT Service**: Peers help each other determine if they are behind NAT and what their external address is.

3. **UPnP/NAT-PMP**: Automatically configures port forwarding on compatible routers.

4. **Hole Punching (DCUtR)**: Direct Connection Upgrade through Relay - allows peers behind NAT to establish direct connections.

5. **Circuit Relay**: Use other peers as relays when direct connection is not possible.

## Solutions

### Option 1: Enable AutoNAT Service (Recommended for nodes behind NAT)

```conf
# In settings_local.conf
p2p_enable_nat_service = true
```

This enables libp2p's AutoNAT service which:

- Helps determine your external address
- Allows the node to learn its public IP from other peers
- Automatically adds observed addresses to the advertised list

### Option 2: Explicitly Set Advertise Address

If you know your public IP or domain:

```conf
# In settings_local.conf
p2p_advertise_addresses = ["/ip4/203.0.113.1/tcp/9905"]
```

This is ideal for:

- Nodes with static public IPs
- Kubernetes deployments with known ingress IPs
- Nodes behind reverse proxies

### Option 3: Enable NAT Port Mapping

For routers that support UPnP or NAT-PMP:

```conf
# In settings_local.conf
p2p_enable_nat_port_map = true
```

### Option 4: Enable Full NAT Traversal Suite

For maximum connectivity in challenging network environments:

```conf
# In settings_local.conf
p2p_enable_nat_service = true
p2p_enable_nat_port_map = true
p2p_enable_hole_punching = true
p2p_enable_relay = true
```

### Option 5: Share Private Addresses (Development Only)

For local development or private networks:

```conf
# In settings_local.conf
p2p_share_private_addresses = true
```

⚠️ **Warning**: Only use this in controlled environments where peers can actually reach private IPs.

## How the Observed Address Mechanism Works

1. Node A (behind NAT) connects outbound to Node B
2. Node B sees the connection coming from Node A's public IP (e.g., 203.0.113.1:45678)
3. Node B tells Node A: "I see you at 203.0.113.1:45678" via the Identify protocol
4. Node A adds this "observed address" to its list of advertised addresses
5. Node A can now tell other peers: "You can reach me at 203.0.113.1:45678"
6. Other peers can now connect to Node A using this address

## Debugging

To debug NAT traversal issues:

1. Check the logs for warnings about unreachable nodes:

    ```bash
    grep "No peers connected\|NAT\|observed\|advertise" teranode.log
    ```

2. Verify your configuration:

    ```bash
    grep "p2p_enable_nat\|p2p_advertise" settings_local.conf
    ```

3. Test connectivity from another node:

    ```bash
    telnet <your-public-ip> 9905
    ```

## Best Practices

1. **For Production**: Always set explicit `p2p_advertise_addresses` with your public IP
2. **For Cloud/Kubernetes**: Use the load balancer or ingress IP in `p2p_advertise_addresses`
3. **For Development**: Enable `p2p_enable_nat_service` for automatic address detection
4. **For Home Networks**: Enable `p2p_enable_nat_port_map` if your router supports it

## Implementation Details

### Address Advertisement vs Filtering

The solution involves two stages:

1. **Advertisement Stage**: Nodes advertise their listen addresses (including private IPs) to enable libp2p's observed address detection
2. **Filtering Stage**: When sharing peer lists via GetPeers, nodes filter out private addresses unless explicitly configured to share them

### Why This Approach Works

- Nodes behind NAT advertise their private addresses
- When they connect to public nodes, those nodes observe the NAT's public IP
- The public nodes tell the NAT'd node its observed public address via libp2p's Identify protocol
- The NAT'd node can then share this observed public address with other peers
- Private addresses are filtered when sharing peer lists (not when advertising own addresses)

### The go-p2p v1.1.18 Issue

The go-p2p library's AddrsFactory aggressively filters private IPs when advertiseAddresses is empty. This prevents the observed address mechanism from working because:

- The node has no addresses to advertise
- Peers can't tell it what public address they observe
- The node remains invisible to the network

The solution is to always pass listen addresses to override this filtering, allowing the observed address mechanism to function properly.
