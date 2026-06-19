# How to Expose the P2P Service Publicly (Kubernetes)

Last modified: 11-June-2026

## Why This Matters

Teranode's P2P layer (libp2p) needs inbound TCP connections on the P2P port (`9905` in the shipped configuration) to accept direct peer connections. The port peers actually dial is whatever your `p2p_listen_addresses` / `p2p_advertise_addresses` multiaddrs specify — the load balancer listener port must match the port in your advertise multiaddr. A node that can only dial out still works, but it contributes far less to network health: it cannot be dialed by other peers, cannot serve as a DHT server, and cannot join the BSV Association bootstrap pool (see [Joining the DNS Bootstrap Pool](#joining-the-dns-bootstrap-pool)).

Inside Kubernetes this is not automatic. Pod IPs are not routable from the internet, and the default Service behaviour (`externalTrafficPolicy: Cluster`) breaks libp2p in subtle ways described below. This guide shows a working pattern for AWS/EKS; the principles carry over to other cloud providers.

## AWS/EKS: Network Load Balancer

Use a **Network Load Balancer (NLB)**, not an ALB or Classic ELB. libp2p speaks a raw TCP protocol with its own multiplexing and encryption — an L7 load balancer cannot proxy it. The NLB operates at L4 and passes TCP streams through untouched.

### Service Manifest

The example below uses [AWS Load Balancer Controller](https://kubernetes-sigs.github.io/aws-load-balancer-controller/) annotations (`aws-load-balancer-type: "external"`, `aws-load-balancer-nlb-target-type`) — install the controller in your cluster as a prerequisite. Adapt the `selector` to whatever labels your peer pod actually carries.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: teranode-p2p-nlb
  annotations:
    # Use NLB for better performance with libp2p
    service.beta.kubernetes.io/aws-load-balancer-type: "external"
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: "instance"
    service.beta.kubernetes.io/aws-load-balancer-scheme: "internet-facing"
spec:
  type: LoadBalancer
  externalTrafficPolicy: Local
  # With Local policy, the NLB only routes to nodes running a peer pod.
  # This prevents cross-node forwarding issues with libp2p.
  selector:
    app: peer
  ports:
    - port: 9905
      targetPort: 9905
      protocol: TCP
      name: teranode-p2p
```

### Why `externalTrafficPolicy: Local`

This setting is **required**, not an optimisation. With the default `Cluster` policy:

- The NLB routes incoming connections to *any* node in the cluster, and `kube-proxy` forwards them to the peer pod with source NAT applied.
- Peers connecting to your node appear to come from your own cluster node IPs, not their real addresses. This breaks libp2p's Identify/observed-address mechanism (see [P2P NAT Traversal](../../../P2P_NAT_TRAVERSAL.md)), peer-level bans, and any source-IP-based diagnostics.
- The extra cross-node hop adds latency and a second failure point on every inbound stream.

With `Local`:

- The NLB health checks fail on nodes without a peer pod, so traffic only ever lands on the node actually running the pod — no second hop, no SNAT.
- The peer pod sees the genuine source IP of every connecting peer.

!!! warning "Pod scheduling caveat"
    With `Local` policy, a node passes NLB health checks only while it hosts a peer pod. If the pod is rescheduled, expect a brief window of failed health checks while the NLB target group converges. Keep the peer pod stable (e.g. node affinity or a dedicated node group) to avoid flapping.

The example above uses `aws-load-balancer-nlb-target-type: "instance"`. Target type `ip` (routing straight to the pod ENI, requires the AWS Load Balancer Controller) also works and likewise preserves source IPs; `instance` + `Local` is the simpler, proven combination.

### Advertise the Load Balancer Address

The pod listens on its pod IP, so left to its own devices it advertises an unroutable address. Point `p2p_advertise_addresses` at the NLB:

```text
# Using the NLB DNS name
p2p_advertise_addresses = /dns4/<your-nlb-or-route53-name>/tcp/9905

# Or, if you attached an Elastic IP to the NLB
p2p_advertise_addresses = /ip4/<elastic-ip>/tcp/9905
```

A stable DNS name (e.g. via external-dns and Route 53) or an Elastic IP attached to the NLB is strongly recommended — the auto-generated NLB DNS name changes if the Service is recreated, and a changed advertise address makes your node temporarily undialable until peers rediscover it.

### Verify Reachability

From a machine **outside** your VPC:

```bash
# TCP-level check: the NLB should accept the connection
nc -vz <your-nlb-dns-name> 9905
```

Then confirm peers are dialing in: inbound connections appear in the P2P service logs, and other nodes should list yours in their peer set. If outbound peering works but you never see inbound connections, re-check the NLB target health and that `p2p_advertise_addresses` matches the address that is actually reachable.

## Choosing a DHT Mode

`p2p_dht_mode` controls how visible — and how expensive — your node is on the P2P layer:

| Mode | Behaviour | Cost / implications |
|------|-----------|---------------------|
| `server` | Full DHT participation: advertises your node, stores provider records, routes queries for other peers | Highest resource usage (memory, CPU, connection count). Maximises direct connections and network contribution. Only useful if your node is publicly dialable as described above. |
| `client` | Queries the DHT for discovery but does not advertise or store records | Still connects to 100+ peers for DHT routing. Suitable for development and home networks. |
| `off` | No DHT. Topic-only network: connects to bootstrap peers and topic peers, exchanging addresses via the bootstrap servers | Most lightweight — no DHT crawling, no random connections. |

Set `p2p_dht_mode = server` when you have completed the NLB setup above and want to strengthen the P2P layer with as many direct connections as possible.

!!! note "Operator-managed deployments"
    Deployments managed by the teranode-operator use the `.operator` settings context, which already defaults to `p2p_dht_mode = server` (`p2p_dht_mode.operator = server` in `settings.conf`). You only need to set the mode explicitly to *opt out* of server mode on such deployments. A `server` node behind a broken or missing public exposure actively *harms* discovery: it advertises addresses into the DHT that nobody can dial.

!!! warning "Abuse-sensitive hosting providers (Hetzner, OVH, …)"
    `server` and `client` modes connect to 100+ semi-random IPs as part of normal DHT operation. Some hosting providers (notably Hetzner and OVH) flag this pattern as network scanning and may send abuse warnings or suspend the server. On such providers, run `p2p_dht_mode = off` — the node still participates fully in block and transaction propagation via topics, using peer address exchange through the bootstrap servers instead of DHT crawling.

In `server` mode, DHT maintenance overhead can be tuned with `p2p_dht_cleanup_interval` (default `24h`).

## Joining the DNS Bootstrap Pool

The default bootstrap peer list is resolved dynamically via DNS:

```text
p2p_bootstrap_peers = /dnsaddr/${network}.bootstrap.teranode.bsvb.tech
```

For example, mainnet nodes bootstrap from `mainnet.bootstrap.teranode.bsvb.tech`.

This pool is managed by the BSV Association. If your node:

1. is publicly dialable on its P2P port (e.g. via the NLB setup above), and
2. runs `p2p_dht_mode = server`,

you can contact the BSV Association to have it added to the bootstrap pool. Bootstrap nodes are the entry points every new node uses to join the network — a larger, geographically diverse pool makes the network more resilient.

## Other Cloud Providers

The same principles apply outside AWS:

- Use an **L4 (TCP) load balancer** — never an HTTP/L7 proxy — for the P2P port.
- Set `externalTrafficPolicy: Local` on the Service to preserve source IPs and avoid cross-node forwarding.
- Advertise the load balancer's stable public address via `p2p_advertise_addresses`.
- Verify inbound reachability from outside the provider's network before enabling `p2p_dht_mode = server`.

## Related Documentation

- [P2P NAT Traversal and Address Filtering](../../../P2P_NAT_TRAVERSAL.md) — how libp2p address discovery works
- [P2P Settings Reference](../../../references/settings/services/p2p_settings.md) — all P2P configuration options
- [Security Best Practices (Kubernetes)](minersSecurityBestPractices.md) — firewall and port exposure guidance
