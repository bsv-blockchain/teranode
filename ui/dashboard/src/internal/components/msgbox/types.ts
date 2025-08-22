export enum MessageType {
  block = 'block',
  mining_on = 'mining_on',
  subtree = 'subtree',
  ping = 'ping',
  getminingcandidate = 'getminingcandidate',
  node_status = 'node_status',
}

interface P2PMessageBase {
  type: MessageType
  hash?: string
  base_url: string
  peer_id: string
  receivedAt: Date
}

export interface BlockMessage extends P2PMessageBase {
  type: MessageType.block
  hash: string
  timestamp: number
}

export interface MiningOnMessage extends P2PMessageBase {
  type: MessageType.mining_on
  hash: string
  previousblockhash: string
  tx_count: number
  size_in_bytes: number
  height: number
  miner: string
}

export interface SubtreeMessage extends P2PMessageBase {
  type: MessageType.subtree
  hash: string
}

export interface PingMessage {
  type: MessageType.ping
  base_url: string
  receivedAt: Date
}

// BlockAssembly details structure - matches blockassembly_api.StateMessage
export interface BlockAssemblyDetails {
  blockAssemblyState?: string      // State of the block assembly service
  subtreeProcessorState?: string   // State of the block assembly subtree processor
  resetWaitCount?: number          // Number of blocks the reset has to wait for
  resetWaitTime?: number           // Time in seconds the reset has to wait for
  subtreeCount?: number            // Number of subtrees
  txCount?: number                 // Number of transactions
  queueCount?: number              // Size of the queue
  currentHeight?: number           // Height of the chaintip
  currentHash?: string             // Hash of the chaintip
  removeMapCount?: number          // Number of transactions in the remove map
  subtrees?: string[]              // Hashes of the current subtrees
}

export interface NodeStatusMessage extends P2PMessageBase {
  type: MessageType.node_status
  version: string
  commit_hash: string
  best_block_hash: string
  best_height: number
  block_assembly_details?: BlockAssemblyDetails // New field
  fsm_state: string
  start_time: number
  uptime: number
  client_name?: string
  miner_name?: string
  listen_mode: string
  chain_work?: string
  sync_peer_id?: string
  sync_peer_height?: number
  sync_peer_block_hash?: string
  sync_connected_at?: number
}

export type P2PMessage =
  | BlockMessage
  | MiningOnMessage
  | SubtreeMessage
  | PingMessage
  | NodeStatusMessage

export interface StatusMessage {
  timestamp: string
  type: string
  source: MessageSource
  subtype: string
  value: string
  base_url: string
  latency: number
}

export type Message = P2PMessage | StatusMessage

export type MessageSource = 'p2p' | 'status'

export interface MsgDisplayField {
  label: string
  value: any
}
