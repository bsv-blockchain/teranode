<script lang="ts">
  import { onMount } from 'svelte'
  import { humanTime } from '$internal/utils/format'
  import { getMessageFields } from './utils'
  import { MessageType } from './types'
  import type { Message, P2PMessage, StatusMessage, MessageSource, MsgDisplayField } from './types'
  import i18n from '../../i18n'

  export let source: MessageSource = 'p2p'
  export let message: Message
  export let collapse = false
  export let titleMinW = '120px'

  let age = ''
  let fields: MsgDisplayField[] = []

  function getMsgDateMillis(source: MessageSource, msg: Message) {
    if (source === 'p2p') {
      return (message as P2PMessage).receivedAt
        ? (message as P2PMessage).receivedAt.getTime()
        : null
    } else if (source === 'status') {
      return (message as StatusMessage).timestamp
        ? new Date((message as StatusMessage).timestamp).getTime()
        : null
    }
    return null
  }

  onMount(() => {
    let millis: number | null = getMsgDateMillis(source, message)
    age = millis ? humanTime(millis) : ''
    fields = getMessageFields(source, message, `${age} ago`)

    const interval = setInterval(() => {
      millis = getMsgDateMillis(source, message)
      age = millis ? humanTime(millis) : ''
    }, 1000)

    return () => clearInterval(interval)
  })

  $: updatedFields = getMessageFields(source, message, `${age} ago`)
  $: baseKey = `comp.msgbox.${message.type.toLowerCase()}`

  $: title =
    source === 'status' && (message as StatusMessage).source === 'status'
      ? $i18n.t(`comp.msgbox.status.type.${message.type.toLowerCase()}.title`)
      : $i18n.t(`${baseKey}.title`)
</script>

<div
  class="msgbox"
  style:--bg-color={`var(--msgbox-${
    MessageType[message.type.toLowerCase()] ? message.type.toLowerCase() : 'default'
  }-bg-color)`}
  style:--title-min-width={titleMinW}
  class:collapse
>
  <div class="title">{title}</div>
  <div class="content">
    {#each updatedFields as field (field.label)}
      <div class="entry">
        <div class="label">
          {field.label}
        </div>
        <div class="value">
          {field.value}
        </div>
      </div>
    {/each}
  </div>
</div>

<style>
  .msgbox {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: flex-start;

    width: 100%;
    min-height: 40px;
    padding: 12px 16px;

    border-radius: 12px;
    border: 1px solid rgba(255, 255, 255, 0.11);
    background: var(--bg-color);
  }
  .msgbox.collapse {
    flex-direction: column;
    gap: 8px;
  }

  .title {
    box-sizing: var(--box-sizing);

    flex: 0;
    min-width: var(--title-min-width);
    word-wrap: break-word;

    color: rgba(10, 17, 24, 0.88);

    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .content {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
  .msgbox.collapse .content {
    gap: 6px;
  }

  .entry {
    display: flex;
    align-items: flex-start;

    color: rgba(10, 17, 24, 0.88);

    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 400;
    line-height: 18px;
    letter-spacing: 0.26px;
  }
  .msgbox.collapse .entry {
    flex-direction: column;
  }

  .label {
    min-width: 135px;
    transition: min-width var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
    word-break: break-all;
  }
  .msgbox.collapse .label {
    min-width: 135px;
  }
  .value {
    word-break: break-all;
  }
</style>
