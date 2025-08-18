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
  export let hidePeer = false
  export let rawMode = false

  let age = ''
  let fields: MsgDisplayField[] = []
  let title = ''
  
  // Format JSON with syntax highlighting
  function formatJSON(obj: any): string {
    try {
      let json = JSON.stringify(obj, null, 2)
      
      // Basic syntax highlighting
      json = json
        // Strings (but not property names)
        .replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>')
        // Numbers
        .replace(/: (\d+)/g, ': <span class="json-number">$1</span>')
        // Booleans
        .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
        // Null
        .replace(/: (null)/g, ': <span class="json-null">$1</span>')
        // Property names
        .replace(/"([^"]+)":/g, '<span class="json-key">"$1"</span>:')
        
      return json
    } catch (e) {
      return '{}'
    }
  }

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
    fields = getMessageFields(source, message, `${age} ago`, hidePeer)

    const interval = setInterval(() => {
      millis = getMsgDateMillis(source, message)
      age = millis ? humanTime(millis) : ''
    }, 1000)

    return () => clearInterval(interval)
  })

  $: updatedFields = getMessageFields(source, message, `${age} ago`, hidePeer)
  $: baseKey = `comp.msgbox.${message.type.toLowerCase()}`

  $: {
    const translationKey =
      source === 'status' && (message as StatusMessage).source === 'status'
        ? `comp.msgbox.status.type.${message.type.toLowerCase()}.title`
        : `${baseKey}.title`

    // Check if translation exists, if not provide a fallback
    const translation = $i18n.t(translationKey)
    title =
      translation === translationKey
        ? `${message.type.charAt(0).toUpperCase()}${message.type.slice(1).toLowerCase().replace(/_/g, ' ')}`
        : translation
  }
</script>

<div
  class="msgbox"
  style:--border-color={`var(--msgbox-${message.type.toLowerCase()}-border-color, var(--msgbox-default-border-color))`}
  style:--title-min-width={titleMinW}
  class:collapse
>
  <div class="title">{title}</div>
  <div class="content" class:raw-mode={rawMode}>
    {#if rawMode}
      <pre class="json-display">{@html formatJSON(message)}</pre>
    {:else}
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
    {/if}
  </div>
</div>

<style>
  .msgbox {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: flex-start;
    gap: 16px;

    width: 100%;
    min-height: 40px;
    padding: 12px 16px;

    border-radius: 12px;
    border: 2px solid var(--border-color);
    background: var(--msgbox-bg-color);
  }
  .msgbox.collapse {
    flex-direction: column;
    gap: 0;
  }
  .msgbox.collapse .title {
    margin-bottom: 0;
    padding-bottom: 0;
    flex: initial;
    min-width: auto;
  }
  .msgbox.collapse .content {
    margin-top: 0;
    padding-top: 0;
  }

  .title {
    box-sizing: var(--box-sizing);

    flex: 0 0 var(--title-min-width);
    min-width: var(--title-min-width);
    word-wrap: break-word;

    color: var(--border-color);

    font-size: 15px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .content {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }
  .msgbox.collapse .content {
    gap: 6px;
  }
  .content.raw-mode {
    gap: 0;
    width: 100%;
  }

  .entry {
    display: flex;
    align-items: flex-start;

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

    color: var(--msgbox-label-color);
  }
  .msgbox.collapse .label {
    min-width: 135px;
  }
  .value {
    word-break: break-all;
    color: var(--msgbox-value-color);
  }
  
  .json-display {
    margin: 0;
    padding: 0;
    font-family: 'JetBrains Mono', 'Courier New', monospace;
    font-size: 12px;
    line-height: 1.5;
    color: #b4b4b4;
    white-space: pre-wrap;
    word-wrap: break-word;
    overflow-wrap: break-word;
    max-width: 100%;
  }
  
  :global(.json-display .json-key) {
    color: #a8c0ff;
  }
  
  :global(.json-display .json-string) {
    color: #98c379;
  }
  
  :global(.json-display .json-number) {
    color: #d19a66;
  }
  
  :global(.json-display .json-boolean) {
    color: #56b6c2;
  }
  
  :global(.json-display .json-null) {
    color: #abb2bf;
  }
</style>
