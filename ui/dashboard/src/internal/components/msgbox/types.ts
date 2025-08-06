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
  hash: string
  base_url: string
  peer_id: string
  receivedAt: Date
}

export interface BlockMessage extends P2PMessageBase {
  type: MessageType.block
  timestamp: number
}

export interface MiningOnMessage extends P2PMessageBase {
  type: MessageType.mining_on
  previousblockhash: string
  tx_count: number
  size_in_bytes: number
  height: number
  miner: string
}

export interface SubtreeMessage extends P2PMessageBase {
  type: MessageType.subtree
}

export interface PingMessage {
  type: MessageType.ping
  base_url: string
  receivedAt: Date
}

export interface NodeStatusMessage extends P2PMessageBase {
  type: MessageType.node_status
  version: string
  commit_hash: string
  best_block_hash: string
  best_height: number
  tx_count_in_assembly: number
  fsm_state: string
  start_time: number
  uptime: number
  miner_name: string
  listen_mode: string
}

export type P2PMessage = BlockMessage | MiningOnMessage | SubtreeMessage | PingMessage | NodeStatusMessage

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
