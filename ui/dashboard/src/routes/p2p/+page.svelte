<script lang="ts">
  import { afterUpdate } from 'svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import MessageBox from '$internal/components/msgbox/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import { Button, Switch, TextInput } from '$lib/components'
  import { contentLeft } from '$internal/stores/nav'
  import { MessageType } from '$internal/components/msgbox/types'

  import { messages, sock } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  const pageKey = 'page.p2p'

  let innerWidth = 0

  function scrollToTop() {
    if (!import.meta.env.SSR && window && window.scrollTo) {
      // Check if the user is near the top of the scroll (e.g., within the top 100 pixels)
      const scrollThreshold = 100 // Adjust this threshold as needed

      if (window.scrollY <= scrollThreshold) {
        window.scrollTo({ top: 0, behavior: 'smooth' })
      }
    }
  }

  afterUpdate(() => {
    scrollToTop()
  })

  let collapseMsgContent = false

  let byPeer = false
  let filter = ''
  let groupedMessages: any = {}
  let filteredMessages: any[] = []
  let peers: string[] = []

  let dataSnapshot: any = null

  function onLive() {
    if (dataSnapshot) {
      dataSnapshot = null
    } else {
      dataSnapshot = [...$messages]
    }
  }

  $: usingLiveData = dataSnapshot === null

  $: data = dataSnapshot ? dataSnapshot : $messages

  $: {
    // Transoform types to be lower case, as they have been changing case in the BE
    filteredMessages = data.map((item) => ({ ...item, type: item.type.toLowerCase() }))

    if (filter.length > 0) {
      const f = filter.toLowerCase()

      filteredMessages = filteredMessages.filter((message) => {
        const search = JSON.stringify(message).toLowerCase()
        return search.includes(f)
      })
    }

    if (byPeer) {
      let newGroupedMessages: any = {}

      filteredMessages.forEach((message) => {
        if (message.type !== MessageType.ping) {
          if (!newGroupedMessages[message.peer_id]) {
            newGroupedMessages[message.peer_id] = []
          }
          newGroupedMessages[message.peer_id].push(message)
        }
      })

      Object.keys(newGroupedMessages).forEach((peer_id) => {
        newGroupedMessages[peer_id].sort((a: any, b: any) => a.peer_id.localeCompare(b.peerid))
      })

      groupedMessages = newGroupedMessages
    }

    peers = Object.keys(groupedMessages).length > 0 ? Object.keys(groupedMessages) : []
    peers = peers.sort()

    const msgboxW = byPeer ? (innerWidth - $contentLeft) / peers.length : innerWidth - $contentLeft
    collapseMsgContent = msgboxW < 500
  }
</script>

<svelte:window bind:innerWidth />

<PageWithMenu>
  <div class="tools-container">
    <div class="tools">
      <div class="title">{t(`${pageKey}.title`)}</div>
      <div class="filters">
        <Switch
          size="small"
          name="peer"
          label={t(`${pageKey}.by_peer`)}
          bind:checked={byPeer}
          labelPlacement="left"
          labelAlignment="center"
        />

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

  {#if byPeer}
    <div class="container">
      {#each peers as peer}
        <div class="column">
          <div class="peer">
            <Typo
              variant="text"
              size="sm"
              value={peer}
              color="rgba(255, 255, 255, 0.66)"
              wrap={false}
            />
          </div>
          <div class="msg-contaienr">
            {#each groupedMessages[peer] as message}
              <MessageBox {message} source="p2p" collapse={collapseMsgContent} />
            {/each}
          </div>
        </div>
      {/each}
    </div>
  {:else}
    <div class="container">
      <div class="column">
        <div class="msg-contaienr">
          {#each filteredMessages as message}
            <MessageBox {message} source="p2p" collapse={collapseMsgContent} />
          {/each}
        </div>
      </div>
    </div>
  {/if}
</PageWithMenu>

<style>
  .tools-container {
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

  .peer {
    margin-bottom: 5px;
    color: rgba(255, 255, 255, 0.66);
    overflow: hidden;
    text-overflow: ellipsis;
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
