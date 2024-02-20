import * as msg from './types'
import type {
  Message,
  StatusMessage,
  BlockMessage,
  MiningOnMessage,
  SubtreeMessage,
  MessageSource, PingMessage,
} from './types'
import i18n from '../../i18n'

const baseKey = 'comp.msgbox'

let t
i18n.subscribe((value) => {
  t = value.t
})

export const getMessageFields = (source: MessageSource, data: Message, age: string) => {
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
        fields.push({ label: t(`${key}.peer_id`), value: blockMsg.peer_id })
        break
      case msg.MessageType.mining_on:
        const miningOnMsg = data as MiningOnMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.hash`), value: miningOnMsg.hash })
        fields.push({ label: t(`${key}.previousblockhash`), value: miningOnMsg.previousblockhash })
        fields.push({ label: t(`${key}.base_url`), value: miningOnMsg.base_url })
        fields.push({ label: t(`${key}.peer_id`), value: miningOnMsg.peer_id })
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
        fields.push({ label: t(`${key}.peer_id`), value: subtreeMsh.peer_id })
        break
      case msg.MessageType.ping:
        const pingMsg = data as PingMessage
        fields.push({ label: t(`${key}.age`), value: age })
        fields.push({ label: t(`${key}.base_url`), value: pingMsg.base_url })
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
