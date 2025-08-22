import * as msg from './types'
import type {
  Message,
  StatusMessage,
  BlockMessage,
  MiningOnMessage,
  SubtreeMessage,
  MessageSource,
  PingMessage,
  NodeStatusMessage,
  BlockAssemblyDetails,
} from './types'
import i18n from '../../i18n'
import { humanTime } from '$internal/utils/format'

const baseKey = 'comp.msgbox'

let t
i18n.subscribe((value) => {
  t = value.t
})

export const getMessageFields = (
  source: MessageSource,
  data: Message,
  age: string,
  hidePeer: boolean = false,
) => {
  const fields: msg.MsgDisplayField[] = []
  let key = `${baseKey}.${data.type}.fields`
  if (source === 'p2p') {
    switch (data.type) {
      case msg.MessageType.block:
        const blockMsg = data as BlockMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.timestamp`), value: blockMsg.timestamp })
        fields.push({ label: t(`${key}.receivedAt`), value: blockMsg.receivedAt })
        fields.push({ label: t(`${key}.hash`), value: blockMsg.hash })
        fields.push({ label: t(`${key}.base_url`), value: blockMsg.base_url })
        if (!hidePeer) {
          fields.push({ label: t(`${key}.peer_id`), value: blockMsg.peer_id })
        }
        break
      case msg.MessageType.mining_on:
        const miningOnMsg = data as MiningOnMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.hash`), value: miningOnMsg.hash })
        fields.push({ label: t(`${key}.previousblockhash`), value: miningOnMsg.previousblockhash })
        fields.push({ label: t(`${key}.base_url`), value: miningOnMsg.base_url })
        if (!hidePeer) {
          fields.push({ label: t(`${key}.peer_id`), value: miningOnMsg.peer_id })
        }
        fields.push({ label: t(`${key}.tx_count`), value: miningOnMsg.tx_count })
        fields.push({ label: t(`${key}.size_in_bytes`), value: miningOnMsg.size_in_bytes })
        fields.push({ label: t(`${key}.height`), value: miningOnMsg.height })
        fields.push({ label: t(`${key}.miner`), value: miningOnMsg.miner })
        break
      case msg.MessageType.subtree:
        const subtreeMsh = data as SubtreeMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.hash`), value: subtreeMsh.hash })
        fields.push({ label: t(`${key}.base_url`), value: subtreeMsh.base_url })
        if (!hidePeer) {
          fields.push({ label: t(`${key}.peer_id`), value: subtreeMsh.peer_id })
        }
        break
      case msg.MessageType.ping:
        const pingMsg = data as PingMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.base_url`), value: pingMsg.base_url })
        break
      case msg.MessageType.node_status:
        const nodeStatusMsg = data as NodeStatusMessage

        // Format UTC datetime consistently
        const formatUTCDateTime = (date: Date) => {
          const pad = (n: number) => n.toString().padStart(2, '0')
          return (
            `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ` +
            `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`
          )
        }

        // Calculate uptime using humanTime
        const startTime = nodeStatusMsg.start_time * 1000 // Convert to milliseconds
        const uptimeStr = humanTime(startTime) + ' ago'

        // Static data at the top
        const fromWithMiner = nodeStatusMsg.miner_name
          ? `${nodeStatusMsg.base_url} (${nodeStatusMsg.miner_name})`
          : nodeStatusMsg.base_url

        fields.push({ label: t(`${key}.base_url`), value: fromWithMiner })
        fields.push({ label: t(`${key}.commit_hash`), value: nodeStatusMsg.commit_hash })
        fields.push({ label: t(`${key}.version`), value: nodeStatusMsg.version })

        const startTimeStr = formatUTCDateTime(new Date(nodeStatusMsg.start_time * 1000))
        fields.push({ label: t(`${key}.start_time`), value: `${startTimeStr} (${uptimeStr})` })

        // Dynamic data
        fields.push({ label: t(`${key}.best_height`), value: nodeStatusMsg.best_height })
        fields.push({ label: t(`${key}.best_block_hash`), value: nodeStatusMsg.best_block_hash })
        
        // Block Assembly details - display new structure
        if (nodeStatusMsg.block_assembly_details) {
          const assembly = nodeStatusMsg.block_assembly_details
          
          // Main assembly info
          if (assembly.txCount !== undefined) {
            fields.push({ 
              label: 'Block Assembly TXs', 
              value: assembly.txCount.toLocaleString() 
            })
          }
          
          if (assembly.blockAssemblyState) {
            fields.push({ 
              label: 'Assembly State', 
              value: assembly.blockAssemblyState 
            })
          }
          
          if (assembly.subtreeCount !== undefined) {
            fields.push({ 
              label: 'Subtree Count', 
              value: assembly.subtreeCount.toLocaleString() 
            })
          }
          
          if (assembly.currentHeight !== undefined) {
            fields.push({ 
              label: 'Assembly Height', 
              value: assembly.currentHeight.toLocaleString() 
            })
          }
          
          if (assembly.currentHash) {
            // Show truncated hash for readability
            const shortHash = assembly.currentHash.substring(0, 12) + '...'
            fields.push({ 
              label: 'Assembly Hash', 
              value: shortHash
            })
          }
          
          // Show number of subtrees if array exists
          if (assembly.subtrees && assembly.subtrees.length > 0) {
            fields.push({ 
              label: 'Subtrees in Assembly', 
              value: `${assembly.subtrees.length} subtrees` 
            })
          }
          
          // Show additional state info if available
          if (assembly.subtreeProcessorState) {
            fields.push({
              label: 'Subtree Processor State',
              value: assembly.subtreeProcessorState
            })
          }
          
          if (assembly.queueCount !== undefined && assembly.queueCount > 0) {
            fields.push({
              label: 'Queue Size',
              value: assembly.queueCount.toLocaleString()
            })
          }
        }
        
        fields.push({ label: t(`${key}.fsm_state`), value: nodeStatusMsg.fsm_state })
        fields.push({ label: t(`${key}.listen_mode`), value: nodeStatusMsg.listen_mode })

        // Add peer_id if not hidden
        if (!hidePeer && nodeStatusMsg.peer_id) {
          fields.push({ label: t(`${key}.peer_id`), value: nodeStatusMsg.peer_id })
        }

        // Received at with age
        const receivedStr = formatUTCDateTime(nodeStatusMsg.receivedAt)
        fields.push({ label: t(`${key}.receivedAt`), value: `${receivedStr} (${age})` })
        fields.push({ label: t(`${key}.age`), value: age })
        break
      default:
        // Handle unknown message types with generic fields
        const genericMsg = data as any
        fields.push({ label: t(`${baseKey}.ping.fields.age`), value: age })
        if (genericMsg.hash) fields.push({ label: 'Hash', value: genericMsg.hash })
        if (genericMsg.base_url) fields.push({ label: 'From', value: genericMsg.base_url })
        if (genericMsg.receivedAt)
          fields.push({ label: 'Received at', value: genericMsg.receivedAt })
        break
    }
  } else if (source === 'status') {
    const statusMsg = data as StatusMessage
    key = `${baseKey}.status.fields`
    fields.push({ label: t(`${key}.timestamp`), value: statusMsg.timestamp })
    fields.push({ label: t(`${key}.type`), value: statusMsg.type })
    fields.push({ label: t(`${key}.source`), value: statusMsg.source })
    fields.push({ label: t(`${key}.subtype`), value: statusMsg.subtype })
    fields.push({ label: t(`${key}.value`), value: statusMsg.value })
    fields.push({ label: t(`${key}.base_url`), value: statusMsg.base_url })
    fields.push({ label: t(`${key}.latency`), value: statusMsg.latency })
  }
  return fields
}
