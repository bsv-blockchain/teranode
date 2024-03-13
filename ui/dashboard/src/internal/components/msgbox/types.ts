export enum MessageType {
  block = 'block',
  mining_on = 'mining_on',
  subtree = 'subtree',
  ping = 'ping',
  getminingcandidate = 'getminingcandidate',
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

export type P2PMessage = BlockMessage | MiningOnMessage | SubtreeMessage | PingMessage

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
