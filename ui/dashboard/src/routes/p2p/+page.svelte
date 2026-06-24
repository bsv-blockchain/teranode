<svelte:options runes={true} />

<script lang="ts">
  import { untrack } from 'svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import MessageBox from '$internal/components/msgbox/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import { Button, Switch, TextInput, Dropdown } from '$lib/components'
  import { contentLeft } from '$internal/stores/nav'
  import { MessageType } from '$internal/components/msgbox/types'

  import { messages, sock, connectionAttempts, currentNodePeerID } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  const t = $derived($i18n.t)

  const connected = $derived($sock !== null)

  const pageKey = 'page.p2p'

  let innerWidth = $state(0)

  function scrollToTop() {
    if (!import.meta.env.SSR && window && window.scrollTo) {
      // Check if the user is near the top of the scroll (e.g., within the top 100 pixels)
      const scrollThreshold = 100 // Adjust this threshold as needed

      if (window.scrollY <= scrollThreshold) {
        window.scrollTo({ top: 0, behavior: 'smooth' })
      }
    }
  }

  let collapseMsgContent = $state(false)

  let byPeer = $state(false)
  let rawMode = $state(false) // Toggle for showing raw JSON
  let filter = $state('')
  let showLocalMessages = $state(false) // Toggle for showing local messages (off by default)
  let groupedMessages: any = $state({})
  let filteredMessages: any[] = $state([])
  let peers: string[] = $state([])
  let peerClientNames: { [key: string]: string } = $state({}) // Map peer IDs to client names

  // In by-peer mode, always collapse messages for compact view
  const actualCollapseState = $derived(byPeer ? true : collapseMsgContent)

  // Message type filter
  const messageTypeSet = new Set(['All'])
  let messageTypeOptions: string[] = $state(['All'])
  let selectedMessageType = $state('All')
  let reverseFilter = $state(false)

  // Function to process message types
  function processMessageType(messageType: string) {
    if (messageType && !messageTypeSet.has(messageType)) {
      messageTypeSet.add(messageType)
      // Rebuild options array only when a new type is discovered
      messageTypeOptions = [
        'All',
        ...Array.from(messageTypeSet)
          .filter((t) => t !== 'All')
          .sort(),
      ]
    }
  }

  let dataSnapshot: any = $state(null)

  function onLive() {
    if (dataSnapshot) {
      dataSnapshot = null
    } else {
      dataSnapshot = [...$messages]
    }
  }

  const usingLiveData = $derived(dataSnapshot === null)

  const data = $derived(dataSnapshot ? dataSnapshot : $messages)

  $effect(() => {
    // Read all genuine reactive inputs up front so the effect re-runs when they
    // change. The body then runs untracked, so writing to (and re-reading)
    // filteredMessages / groupedMessages / peers does not self-trigger the effect.
    const inData = data
    const inShowLocal = showLocalMessages
    const inCurrentPeer = $currentNodePeerID
    const inSelectedType = selectedMessageType
    const inReverse = reverseFilter
    const inFilter = filter
    const inByPeer = byPeer
    const inInnerWidth = innerWidth
    const inContentLeft = $contentLeft

    untrack(() => {
      // Transform types to be lower case, as they have been changing case in the BE
      // Filter messages based on the showLocalMessages toggle
      let newFiltered = inData
        .filter((item) => {
          // Only filter out local messages if showLocalMessages is false
          if (!inShowLocal && inCurrentPeer) {
            // Check if the message is from the current node using peer_id
            const messagePeerId = item.peer_id || item.peerID || item.peer
            return messagePeerId !== inCurrentPeer // Keep message if it's NOT from current node
          }
          return true
        })
        .map((item) => {
          // Process message type for dropdown
          const lowerType = item.type?.toLowerCase()
          processMessageType(lowerType)
          return { ...item, type: lowerType }
        })

      // Apply message type filter
      if (inSelectedType !== 'All') {
        if (inReverse) {
          newFiltered = newFiltered.filter((msg) => msg.type !== inSelectedType)
        } else {
          newFiltered = newFiltered.filter((msg) => msg.type === inSelectedType)
        }
      }

      if (inFilter.length > 0) {
        const f = inFilter.toLowerCase()

        newFiltered = newFiltered.filter((message) => {
          // More targeted search - check specific fields instead of entire JSON
          return (
            message.type?.toLowerCase().includes(f) ||
            message.hash?.toLowerCase().includes(f) ||
            message.base_url?.toLowerCase().includes(f) ||
            message.miner?.toLowerCase().includes(f) ||
            message.miner_name?.toLowerCase().includes(f) ||
            message.client_name?.toLowerCase().includes(f) ||
            message.peer_id?.toLowerCase().includes(f) ||
            message.peerID?.toLowerCase().includes(f) ||
            message.peer?.toLowerCase().includes(f) ||
            message.fsm_state?.toLowerCase().includes(f) ||
            message.version?.toLowerCase().includes(f)
          )
        })
      }

      filteredMessages = newFiltered

      if (inByPeer) {
        let newGroupedMessages: any = {}
        let newPeerClientNames: { [key: string]: string } = {}

        newFiltered.forEach((message) => {
          // Compare with lowercase since we convert types to lowercase
          if (message.type !== MessageType.ping.toLowerCase()) {
            // Check for peer_id, peerID, or peer field
            const peerId = message.peer_id || message.peerID || message.peer || 'unknown'

            if (!newGroupedMessages[peerId]) {
              newGroupedMessages[peerId] = []
            }
            newGroupedMessages[peerId].push(message)

            // Extract client name if available and not already stored
            if (!newPeerClientNames[peerId] && message.client_name) {
              newPeerClientNames[peerId] = message.client_name
            }
          }
        })

        // Sort messages in descending order by receivedAt timestamp
        Object.keys(newGroupedMessages).forEach((peer_id) => {
          newGroupedMessages[peer_id].sort((a: any, b: any) => {
            // Sort by receivedAt in descending order (newest first)
            return new Date(b.receivedAt).getTime() - new Date(a.receivedAt).getTime()
          })
        })

        groupedMessages = newGroupedMessages
        peerClientNames = newPeerClientNames
      }

      const newPeers =
        Object.keys(groupedMessages).length > 0 ? Object.keys(groupedMessages).sort() : []
      peers = newPeers

      const msgboxW = inByPeer
        ? (inInnerWidth - inContentLeft) / newPeers.length
        : inInnerWidth - inContentLeft
      collapseMsgContent = msgboxW < 500
    })
  })

  // Scroll to top after the message list re-renders (replaces afterUpdate)
  $effect(() => {
    // Touch the reactive values that drive a re-render so this fires after them
    void filteredMessages
    void groupedMessages
    void byPeer
    scrollToTop()
  })
</script>

<svelte:window bind:innerWidth />

<PageWithMenu testId="page-root">
  <div class="tools-container">
    <div class="tools">
      <div class="title">
        {t(`${pageKey}.title`)}
        {#if $connectionAttempts > 0 && !connected}
          <span class="connection-error"
            >P2P connection failed. Attempt {$connectionAttempts}/5</span
          >
        {/if}
      </div>
      <div class="filters">
        <Switch
          size="small"
          name="peer"
          label={t(`${pageKey}.by_peer`)}
          bind:checked={byPeer}
          labelPlacement="left"
          labelAlignment="center"
        />

        <Switch
          size="small"
          name="localMessages"
          label="Show Local Messages"
          bind:checked={showLocalMessages}
          labelPlacement="left"
          labelAlignment="center"
        />

        <Switch
          size="small"
          name="rawMode"
          label="Raw JSON"
          bind:checked={rawMode}
          labelPlacement="left"
          labelAlignment="center"
        />

        <div class="filter-group">
          <span class="filter-label">Filter:</span>
          <Dropdown
            size="small"
            name="messageType"
            bind:value={selectedMessageType}
            items={messageTypeOptions.map((type) => ({ value: type, label: type }))}
          />
          <Switch
            size="small"
            name="reverseFilter"
            label="Reverse"
            bind:checked={reverseFilter}
            labelPlacement="left"
            labelAlignment="center"
            disabled={selectedMessageType === 'All'}
          />
          <TextInput size="small" name="filter" placeholder="Search..." bind:value={filter} />
        </div>

        <Button
          size="small"
          icon="icon-status-light-glow-solid"
          iconColor={connected ? '#15B241' : '#CE1722'}
          uppercase={true}
          onclick={onLive}
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
          <div class="peer" title={peer}>
            <Typo
              variant="text"
              size="sm"
              value={peerClientNames[peer] || '(not set)'}
              color="var(--comp-label-color)"
              wrap={false}
            />
          </div>
          <div class="msg-contaienr">
            {#each groupedMessages[peer] as message, index (peer + '_' + index + '_' + (message.hash || 'no-hash') + '_' + message.receivedAt)}
              <MessageBox
                {message}
                source="p2p"
                collapse={actualCollapseState}
                titleMinW={actualCollapseState ? 'auto' : '80px'}
                hidePeer={true}
                {rawMode}
              />
            {/each}
          </div>
        </div>
      {/each}
    </div>
  {:else}
    <div class="container">
      <div class="column column-full">
        <div class="msg-contaienr">
          {#each filteredMessages as message, index ('msg_' + index + '_' + (message.hash || 'no-hash') + '_' + message.receivedAt)}
            <MessageBox
              {message}
              source="p2p"
              collapse={actualCollapseState}
              titleMinW={actualCollapseState ? 'auto' : '120px'}
              {rawMode}
            />
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
    background: var(--comp-bg-color);
  }

  .tools {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    justify-content: space-between;

    margin-top: -8px;
  }
  .tools .title {
    color: var(--app-color);

    font-family: var(--font-family);
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;

    margin-top: 8px;
    display: flex;
    align-items: center;
    gap: 20px;
  }

  .connection-error {
    color: #ff6b6b;
    font-size: 14px;
    font-weight: 400;
  }
  .tools .filters {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    flex-wrap: wrap;
    gap: 15px;

    margin-top: 8px;
  }

  .filter-group {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .filter-label {
    color: var(--app-color);
    font-size: 14px;
    font-weight: 500;
  }

  .peer {
    margin-bottom: 5px;
    color: var(--comp-label-color);
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
    flex: 0 0 auto;
    min-width: 280px;
    max-width: 350px;
  }

  .column-full {
    flex: 1;
    min-width: unset;
    max-width: unset;
  }

  .column .peer {
    padding: 8px 12px;
    background: var(--app-subtle-bg-color);
    border-radius: 8px;
    margin-bottom: 12px;
    text-align: center;
    font-weight: 600;
  }

  * {
    box-sizing: var(--box-sizing);
  }
</style>
