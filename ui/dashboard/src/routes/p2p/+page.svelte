<script lang="ts">
  import { afterUpdate, onMount } from 'svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import MessageBox from '$internal/components/msgbox/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import { Button, Switch, TextInput, Dropdown } from '$lib/components'
  import { contentLeft } from '$internal/stores/nav'
  import { MessageType } from '$internal/components/msgbox/types'

  import { messages, sock, connectionAttempts } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  const pageKey = 'page.p2p'

  let innerWidth = 0
  let currentNodeUrl = ''

  // Get the current node's URL to filter out self-messages
  onMount(async () => {
    if (!import.meta.env.SSR && window && window.location) {
      try {
        // Try to get the node configuration from the backend
        const response = await fetch('/api/config/node')
        if (response.ok) {
          const config = await response.json()
          if (config.nodeBaseUrl) {
            currentNodeUrl = config.nodeBaseUrl
          } else {
            throw new Error('No nodeBaseUrl in config')
          }
        } else {
          throw new Error('Config endpoint not available')
        }
      } catch (error) {
        currentNodeUrl = '' // Disable filtering if we can't determine the node URL
      }
    }
  })

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

  // In by-peer mode, always collapse messages for compact view
  $: actualCollapseState = byPeer ? true : collapseMsgContent

  let byPeer = false
  let rawMode = false // Toggle for showing raw JSON
  let filter = ''
  let showLocalMessages = false // Toggle for showing local messages (off by default)
  let groupedMessages: any = {}
  let filteredMessages: any[] = []
  let peers: string[] = []
  let peerClientNames: { [key: string]: string } = {} // Map peer IDs to client names

  // Message type filter
  let messageTypeSet = new Set(['All'])
  let messageTypeOptions: string[] = ['All']
  let selectedMessageType = 'All'

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
    // Transform types to be lower case, as they have been changing case in the BE
    // Filter messages based on the showLocalMessages toggle
    filteredMessages = data
      .filter((item) => {
        // Only filter out local messages if showLocalMessages is false
        if (!showLocalMessages && currentNodeUrl && item.base_url) {
          // Extract base URL without path for comparison
          const itemBaseUrl = item.base_url.replace(/\/api\/v1.*$/, '') // Remove /api/v1 and everything after
          const normalizedCurrentUrl = currentNodeUrl.replace(/\/$/, '') // Remove trailing slash if present

          return itemBaseUrl !== normalizedCurrentUrl // Keep message if it's NOT from current node
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
    if (selectedMessageType !== 'All') {
      filteredMessages = filteredMessages.filter((msg) => msg.type === selectedMessageType)
    }

    if (filter.length > 0) {
      const f = filter.toLowerCase()

      filteredMessages = filteredMessages.filter((message) => {
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

    if (byPeer) {
      let newGroupedMessages: any = {}
      let newPeerClientNames: { [key: string]: string } = {}

      filteredMessages.forEach((message, index) => {
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
        } else {
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
          <TextInput size="small" name="filter" placeholder="Search..." bind:value={filter} />
        </div>

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
          <div class="peer" title={peer}>
            <Typo
              variant="text"
              size="sm"
              value={peerClientNames[peer] || '(not set)'}
              color="rgba(255, 255, 255, 0.66)"
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
    color: rgba(255, 255, 255, 0.88);
    font-size: 14px;
    font-weight: 500;
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
    background: rgba(255, 255, 255, 0.05);
    border-radius: 8px;
    margin-bottom: 12px;
    text-align: center;
    font-weight: 600;
  }

  * {
    box-sizing: var(--box-sizing);
  }
</style>
