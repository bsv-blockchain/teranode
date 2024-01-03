<script lang="ts">
  import { onMount } from 'svelte'
  import { MessageType } from '$internal/components/msgbox/types'
  import type { StatusMessage } from '$internal/components/msgbox/types'

  import { Button, TextInput } from '$lib/components'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import MessageBox from '$internal/components/msgbox/index.svelte'
  import { contentLeft } from '$internal/stores/nav'

  import { connectToStatusServer, messages, sock as statusSock } from '$internal/stores/statusStore'

  import {
    messages as p2pMessages,
    connectToP2PServer,
    sock as p2pSock,
  } from '$internal/stores/p2pStore'

  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $p2pSock !== null && $statusSock !== null

  const pageKey = 'page.status'

  let innerWidth = 0

  onMount(async () => {
    connectToStatusServer()
    connectToP2PServer()
  })

  let collapseMsgContent = false
  let filter = ''
  let filteredMessages: any[] = []

  function combineMessages(messages, p2pMessages) {
    const allMessages: Record<string, StatusMessage> = {}

    // normalise type field values
    const statusMsgs = messages.map((item) => ({ ...item, type: item.type.toLowerCase() }))
    const p2pMsgs = p2pMessages.map((item) => ({ ...item, type: item.type.toLowerCase() }))

    statusMsgs.forEach((message) => {
      if (message.type !== MessageType.ping) {
        allMessages[message.clusterName + message.type + message.subtype] = {
          timestamp: message.timestamp || new Date().toISOString(),
          type: message.type,
          source: 'status',
          subtype: message.subtype,
          value: message.value,
          base_url: message.clusterName,
          latency: new Date().getTime() - new Date(message.timestamp).getTime(),
        }
      }
    })

    p2pMsgs.forEach((message) => {
      if (message.type !== MessageType.ping) {
        allMessages[message.base_url + message.type] = {
          timestamp: message.timestamp || new Date().toISOString(),
          type: message.type,
          source: 'p2p',
          subtype: '',
          value: '',
          base_url: message.base_url,
          latency: new Date().getTime() - new Date(message.timestamp).getTime(),
        }
      }
    })

    return allMessages
  }

  let dataSnapshot: any = null

  function onLive() {
    if (dataSnapshot) {
      dataSnapshot = null
    } else {
      dataSnapshot = combineMessages($messages, $p2pMessages)
    }
  }

  $: usingLiveData = dataSnapshot === null

  $: data = dataSnapshot ? dataSnapshot : combineMessages($messages, $p2pMessages)

  $: {
    filteredMessages = Object.values(data)

    if (filter.length > 0) {
      const f = filter.toLowerCase()

      filteredMessages = filteredMessages.filter((message) => {
        const search = JSON.stringify(message).toLowerCase()
        return search.includes(f)
      })
    }

    const msgboxW = innerWidth - $contentLeft
    collapseMsgContent = msgboxW < 500
  }
</script>

<svelte:window bind:innerWidth />

<PageWithMenu>
  <div class="tools-container">
    <div class="tools">
      <div class="title">{t(`${pageKey}.title`)}</div>
      <div class="filters">
        <TextInput
          size="small"
          name="filter"
          placeholder={t(`${pageKey}.filter`)}
          bind:value={filter}
        />

        <Button
          size="small"
          icon="icon-status-light-glow-solid"
          iconColor={connected ? '#15B241' : '#CE1722'}
          uppercase={true}
          on:click={onLive}
        >
          {usingLiveData ? t(`${pageKey}.live`) : t(`${pageKey}.paused`)}
        </Button>
      </div>
    </div>
  </div>

  <div class="container">
    <div class="column">
      <div class="msg-contaienr">
        {#each filteredMessages as message}
          <MessageBox {message} source="status" collapse={collapseMsgContent} />
        {/each}
      </div>
    </div>
  </div>
</PageWithMenu>

<style>
  .tools-container {
    box-sizing: var(--box-sizing);
    width: 100%;

    flex: 1;

    width: 100%;
    min-height: 50px;
    padding: 24px;

    border-radius: 12px;
    background: linear-gradient(0deg, rgba(255, 255, 255, 0.04) 0%, rgba(255, 255, 255, 0.04) 100%),
      #0a1018;
  }

  .tools {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    justify-content: space-between;

    margin-top: -8px;
  }
  .tools .title {
    color: rgba(255, 255, 255, 0.88);

    font-family: var(--font-family);
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;

    margin-top: 8px;
  }
  .tools .filters {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    flex-wrap: wrap;
    gap: 15px;

    margin-top: 8px;
  }

  .container {
    box-sizing: var(--box-sizing);
    margin-top: 20px;

    display: flex;
    align-items: flex-start;
    gap: 10px;

    width: 100%;
    max-width: 100%;
    overflow-x: auto;
  }

  .msg-contaienr {
    flex: 1;

    box-sizing: var(--box-sizing);
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  .column {
    flex: 1;
    min-width: 200px;
  }

  * {
    box-sizing: var(--box-sizing);
  }
</style>
