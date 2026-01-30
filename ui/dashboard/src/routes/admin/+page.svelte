<script lang="ts">
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { onMount, onDestroy } from 'svelte'
  import * as api from '$internal/api'
  import type { FSMState, FSMEvent } from '$internal/api'
  import { failure, success } from '$lib/utils/notifications'
  import i18n from '$internal/i18n'
  import { goto } from '$app/navigation'
  import { logout } from '$internal/stores/authStore'
  import LogoutButton from '../../components/LogoutButton.svelte'
  import { Icon } from '$lib/components'
  import RenderHashWithMiner from '$lib/components/table/renderers/render-hash-with-miner/index.svelte'

  // FSM State Management
  let fsmState: FSMState | null = null
  let fsmEvents: FSMEvent[] = []
  let fsmStates: string[] = []
  let fsmLoading = true
  let fsmError: string | null = null
  let apiBaseUrl = ''
  let pollingInterval: any = null
  let selectedEvent: string | null = null
  const POLLING_INTERVAL_MS = 5000 // 5 seconds

  // Block invalidation
  let blockHash = ''
  let blockActionLoading = false
  let blockActionResult: { success: boolean; message: string } | null = null
  // Create regex pattern for hash validation
  const hashRegex = /^[0-9a-fA-F]{64}$/

  // Invalid blocks list
  let invalidBlocks: any[] = []
  let invalidBlocksLoading = false
  let invalidBlocksError: string | null = null
  let lastInvalidBlocksRefresh: Date | null = null
  let invalidBlocksOffset = 0
  const INVALID_BLOCKS_PAGE_SIZE = 5
  let invalidBlocksHasMore = false

  $: invalidBlocksShowingFrom = invalidBlocks.length > 0 ? invalidBlocksOffset + 1 : 0
  $: invalidBlocksShowingTo = invalidBlocksOffset + invalidBlocks.length

  // Re-validate block state
  let revalidatingBlock = false
  let revalidatingBlockHash = ''

  // Reset peer reputations
  let resettingReputations = false

  // Subscribe to the API base URL
  const unsubscribe = api.assetHTTPAddress.subscribe((value) => {
    apiBaseUrl = value
    console.log('API base URL updated:', apiBaseUrl)
  })

  // Get translation function
  const t = $i18n.t

  function getErrorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error)
  }

  // duplicates the FSM transition logic from the backend (fsm_handler.go 
  // If the backend state machine changes, the UI will become out of sync. 
  function isEventAllowedForState(state: string | undefined, eventName: string): boolean {
    if (!state || !eventName) return false

    switch (state) {
      case 'IDLE':
        return eventName === 'RUN' || eventName === 'LEGACYSYNC'
      case 'RUNNING':
        return eventName === 'STOP' || eventName === 'CATCHUPBLOCKS'
      case 'LEGACYSYNCING':
        return eventName === 'RUN' || eventName === 'STOP'
      case 'CATCHINGBLOCKS':
        return eventName === 'RUN'
      default:
        return false
    }
  }

  async function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  async function refreshFSMStateAfterEvent(previousStateValue: number | undefined) {
    for (let attempt = 0; attempt < 5; attempt++) {
      await fetchFSMState(true)

      if (previousStateValue === undefined || fsmState?.state_value !== previousStateValue) {
        return
      }

      await delay(250 * (attempt + 1))
    }
  }

  function goToPrevInvalidBlocksPage(): void {
    if (invalidBlocksLoading) return
    const nextOffset = Math.max(0, invalidBlocksOffset - INVALID_BLOCKS_PAGE_SIZE)
    fetchInvalidBlocks(nextOffset)
  }

  function goToNextInvalidBlocksPage(): void {
    if (invalidBlocksLoading || !invalidBlocksHasMore) return
    const nextOffset = invalidBlocksOffset + INVALID_BLOCKS_PAGE_SIZE
    fetchInvalidBlocks(nextOffset)
  }

  function getEventDisabledReason(state: string | undefined, eventName: string): string {
    if (!state) return 'FSM state not available'
    if (state === 'CATCHINGBLOCKS' && eventName !== 'RUN') {
      return 'Catchup must complete first'
    }
    return `Not allowed from ${state}`
  }

  function formatTimeAgo(timestamp: number): string {
    const now = Date.now()
    const diffMs = now - timestamp
    const diffSec = Math.floor(diffMs / 1000)
    const diffMin = Math.floor(diffSec / 60)
    const diffHour = Math.floor(diffMin / 60)
    const diffDay = Math.floor(diffHour / 24)

    if (diffSec < 60) {
      return `${diffSec}s ago`
    } else if (diffMin < 60) {
      return `${diffMin}m ago`
    } else if (diffHour < 24) {
      return `${diffHour}h ago`
    } else {
      return `${diffDay}d ago`
    }
  }

  async function fetchFSMState(silent = false) {
    if (!silent) {
      fsmLoading = true
    }

    const previousStateValue = fsmState?.state_value

    try {
      // Log the baseUrl for debugging
      console.log('FSM API baseUrl:', apiBaseUrl)

      const result = await api.getFSMState()
      console.log('FSM State API response:', result)

      if (!result.ok) {
        throw new Error(`Failed to fetch FSM state: ${result.error?.message || 'Unknown error'}`)
      }

      fsmState = result.data

      if (previousStateValue !== undefined && previousStateValue !== fsmState.state_value) {
        await fetchFSMEvents()
      }
    } catch (error: unknown) {
      console.error('Error fetching FSM state:', error)
      fsmError = getErrorMessage(error)
    } finally {
      if (!silent) {
        fsmLoading = false
      }
    }
  }

  // Fetch FSM events
  async function fetchFSMEvents() {
    try {
      const result = await api.getFSMEvents()
      console.log('FSM Events API response:', result)

      if (!result.ok) {
        throw new Error(`Failed to fetch FSM events: ${result.error?.message || 'Unknown error'}`)
      }

      fsmEvents = result.data
    } catch (error: unknown) {
      console.error('Error fetching FSM events:', error)
    }
  }

  async function sendFSMEvent(eventName: string) {
    if (fsmLoading) {
      return
    }

    fsmLoading = true
    const previousStateValue = fsmState?.state_value

    try {
      console.log(`Sending FSM event: ${eventName}`)

      // Use the custom event endpoint for all events
      const result = await api.sendCustomFSMEvent(eventName)

      if (!result.ok) {
        throw new Error(result.error?.message || `Failed to send ${eventName} event`)
      }

      await refreshFSMStateAfterEvent(previousStateValue)

      success(`Successfully sent ${eventName} event. State: ${fsmState?.state}`)
    } catch (error) {
      console.error(`Error sending ${eventName} event:`, error)
      failure(`Failed to send ${eventName} event: ${getErrorMessage(error)}`)

      await fetchFSMState(true)
    } finally {
      fsmLoading = false
    }
  }

  // Map of state values to display names
  const stateNames = {
    0: 'IDLE',
    1: 'RUNNING',
    2: 'CATCHING BLOCKS',
    3: 'LEGACY SYNCING',
    '-1': 'DISCONNECTED',
  }

  // Map of state values to colors
  const stateColors = {
    0: 'gray',
    1: 'green',
    2: 'blue',
    3: 'purple',
    '-1': 'red',
  }

  function getStateLabel(stateValue: number): string {
    return stateNames[stateValue] || `Unknown (${stateValue})`
  }

  function getStateColor(stateValue: number): string {
    return stateColors[stateValue] || '#888'
  }

  function getEventIcon(eventName: string): string {
    if (!eventName) return 'fas fa-question'

    switch (eventName) {
      case 'RUN':
        return 'fas fa-play'
      case 'STOP':
        return 'fas fa-stop'
      case 'CATCHUPBLOCKS':
        return 'fas fa-fast-forward'
      case 'LEGACYSYNC':
        return 'fas fa-sync'
      default:
        return 'fas fa-question'
    }
  }

  function formatEventName(eventName: string): string {
    if (!eventName) return 'Unknown'

    switch (eventName) {
      case 'RUN':
        return 'Run'
      case 'STOP':
        return 'Stop'
      case 'CATCHUPBLOCKS':
        return 'Catch Up'
      case 'LEGACYSYNC':
        return 'Legacy Sync'
      default:
        return eventName
    }
  }

  // Handle logout and redirect to home page
  function handleLogout() {
    logout()
    goto('/')
  }

  async function idleBlockchain() {
    fsmLoading = true
    try {
      const result = await api.idleFSM()
      if (!result.ok) {
        throw new Error(result.error?.message || 'Failed to stop blockchain')
      }
      success('Blockchain is now idle')
      await fetchFSMState()
    } catch (error: unknown) {
      console.error('Error stopping blockchain:', error)
      failure(getErrorMessage(error) || 'Failed to stop blockchain')
    } finally {
      fsmLoading = false
    }
  }

  async function catchupBlockchain() {
    fsmLoading = true
    try {
      const result = await api.catchupFSM()
      if (!result.ok) {
        throw new Error(result.error?.message || 'Failed to start catchup')
      }
      success('Blockchain is now in catchup mode')
      await fetchFSMState()
    } catch (error: unknown) {
      console.error('Error starting catchup:', error)
      failure(getErrorMessage(error) || 'Failed to start catchup')
    } finally {
      fsmLoading = false
    }
  }

  async function legacySyncBlockchain() {
    fsmLoading = true
    try {
      const result = await api.legacySyncFSM()
      if (!result.ok) {
        throw new Error(result.error?.message || 'Failed to start legacy sync')
      }
      success('Blockchain is now in legacy sync mode')
      await fetchFSMState()
    } catch (error: unknown) {
      console.error('Error starting legacy sync:', error)
      failure(getErrorMessage(error) || 'Failed to start legacy sync')
    } finally {
      fsmLoading = false
    }
  }

  async function handleInvalidateBlock() {
    if (!hashRegex.test(blockHash)) {
      failure('Invalid block hash')
      return
    }

    blockActionLoading = true
    blockActionResult = null

    try {
      console.log(`Invalidating block hash: ${blockHash}`)
      const result = await api.invalidateBlock(blockHash)
      if (!result.ok) {
        throw new Error(result.error?.message || 'Failed to invalidate block')
      }

      blockActionResult = { success: true, message: 'Block invalidated successfully' }

      // Refresh the invalid blocks list after a successful action
      await refreshInvalidBlocksWithPageFallback()
    } catch (error: unknown) {
      blockActionResult = {
        success: false,
        message: `Failed to invalidate block: ${getErrorMessage(error)}`,
      }
    } finally {
      blockActionLoading = false
    }
  }

  async function handleRevalidateBlock(hash?: string) {
    const blockHashToUse = hash || blockHash

    if (!hashRegex.test(blockHashToUse)) {
      failure('Invalid block hash')
      return
    }

    blockActionLoading = true

    try {
      console.log(`Revalidating block hash: ${blockHashToUse}`)
      const result = await api.revalidateBlock(blockHashToUse)
      if (!result.ok) {
        const errorMsg = result.error?.message || 'Failed to revalidate block'
        const errorStatus = result.error?.status

        // Handle 404 errors specifically
        if (errorStatus === 404) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        // Check if the error contains "not found" or "does not exist"
        if (
          errorMsg.toLowerCase().includes('not found') ||
          errorMsg.toLowerCase().includes('does not exist')
        ) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        throw new Error(errorMsg)
      }

      // Use the success notification with the hash in the message
      success(
        `Block revalidated successfully: ${blockHashToUse.substring(0, 8)}...${blockHashToUse.substring(blockHashToUse.length - 8)}`,
      )

      // If this was called with the input field hash, clear it
      if (!hash) {
        blockHash = ''
      }

      // Refresh the invalid blocks list after a successful action
      await refreshInvalidBlocksWithPageFallback()
    } catch (error: unknown) {
      failure(`${getErrorMessage(error)}`)
    } finally {
      blockActionLoading = false
    }
  }

  async function revalidateBlock(hash: string) {
    if (revalidatingBlock) return

    revalidatingBlock = true
    revalidatingBlockHash = hash

    try {
      console.log(`Re-validating block hash: ${hash}`)

      // Call the API to re-validate the block
      const result = await api.revalidateBlock(hash)

      if (!result.ok) {
        const errorMsg = result.error?.message || 'Failed to re-validate block'
        const errorStatus = result.error?.status

        // Handle 404 errors specifically
        if (errorStatus === 404) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        // Check if the error contains "not found" or "does not exist"
        if (
          errorMsg.toLowerCase().includes('not found') ||
          errorMsg.toLowerCase().includes('does not exist')
        ) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        throw new Error(errorMsg)
      }

      success(
        `Block re-validated successfully: ${hash.substring(0, 8)}...${hash.substring(hash.length - 8)}`,
      )

      // Refresh the invalid blocks list after a successful action
      await refreshInvalidBlocksWithPageFallback()
    } catch (error: unknown) {
      failure(`${getErrorMessage(error)}`)
    } finally {
      revalidatingBlock = false
      revalidatingBlockHash = ''
    }
  }

  async function performBlockAction(action: (hash: string) => Promise<any>, actionName: string) {
    if (!hashRegex.test(blockHash)) {
      failure('Invalid block hash')
      return
    }

    blockActionLoading = true

    // Store the current hash for the message
    const currentHash = blockHash

    try {
      console.log(`${actionName} block hash: ${currentHash}`)

      // Use the custom event endpoint for all events
      const result = await action(currentHash)

      if (!result.ok) {
        const errorMsg = result.error?.message || `Failed to ${actionName} block`
        const errorStatus = result.error?.status

        // Handle 404 errors specifically
        if (errorStatus === 404) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        // Check if the error contains "not found" or "does not exist"
        if (
          errorMsg.toLowerCase().includes('not found') ||
          errorMsg.toLowerCase().includes('does not exist')
        ) {
          throw new Error(`Block hash not found: This hash is not a valid block in the blockchain`)
        }

        throw new Error(errorMsg)
      }

      // Use the success notification with the hash in the message
      success(
        `Block ${actionName}d successfully: ${currentHash.substring(0, 8)}...${currentHash.substring(currentHash.length - 8)}`,
      )

      // Clear the input field after successful operation
      blockHash = ''

      // Refresh the invalid blocks list after a successful action
      if (actionName === 'invalidate') {
        invalidBlocksOffset = 0
        await refreshInvalidBlocksWithPageFallback(0)
      } else {
        await refreshInvalidBlocksWithPageFallback()
      }
    } catch (error: unknown) {
      failure(`${getErrorMessage(error)}`)
    } finally {
      blockActionLoading = false
    }
  }

  async function fetchInvalidBlocks(offset: number = invalidBlocksOffset) {
    // Save current scroll position before updating
    const scrollPosition = window.scrollY

    invalidBlocksLoading = true
    invalidBlocksError = null

    try {
      const result = await api.getLastInvalidBlocks(INVALID_BLOCKS_PAGE_SIZE, offset)

      if (!result.ok) {
        throw new Error(
          `Failed to fetch invalid blocks: ${result.error?.message || 'Unknown error'}`,
        )
      }

      invalidBlocksOffset = typeof result.data.offset === 'number' ? result.data.offset : offset
      invalidBlocks = result.data.blocks || []
      invalidBlocksHasMore = !!result.data.hasMore
      lastInvalidBlocksRefresh = new Date()
      console.log('Invalid blocks:', invalidBlocks)
    } catch (error: unknown) {
      console.error('Error fetching invalid blocks:', error)
      invalidBlocksError = getErrorMessage(error)
    } finally {
      invalidBlocksLoading = false

      // Restore scroll position after the component updates
      setTimeout(() => {
        window.scrollTo({
          top: scrollPosition,
          behavior: 'instant',
        })
      }, 0)
    }
  }

  async function refreshInvalidBlocksWithPageFallback(startOffset: number = invalidBlocksOffset) {
    await fetchInvalidBlocks(startOffset)

    if (invalidBlocks.length === 0 && invalidBlocksOffset > 0) {
      const prevOffset = Math.max(0, invalidBlocksOffset - INVALID_BLOCKS_PAGE_SIZE)
      if (prevOffset !== invalidBlocksOffset) {
        await fetchInvalidBlocks(prevOffset)
      }
    }
  }

  async function resetPeerReputations() {
    resettingReputations = true

    try {
      const response = await fetch('/api/p2p/reset-reputation', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ peer_id: '' }) // Empty peer_id means reset all
      })

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }

      const result = await response.json()

      if (result.error) {
        throw new Error(result.error)
      }

      success(`Reset reputation for ${result.peers_reset} peers`)
    } catch (error: unknown) {
      console.error('Failed to reset peer reputations:', error)
      failure(`Failed to reset peer reputations: ${getErrorMessage(error)}`)
    } finally {
      resettingReputations = false
    }
  }

  onMount(async () => {
    // Fetch FSM state and related data
    await fetchFSMState()
    await fetchFSMEvents()
    await fetchInvalidBlocks()

    // Set up polling for FSM state
    pollingInterval = setInterval(async () => {
      try {
        await fetchFSMState(true)
      } catch (error) {
        console.error('Error during polling:', error)
        // Don't show failure notifications for polling errors
      }
    }, POLLING_INTERVAL_MS)
  })

  onDestroy(() => {
    // Clean up the subscription and polling interval
    if (unsubscribe) {
      unsubscribe()
    }

    if (pollingInterval) {
      clearInterval(pollingInterval)
    }
  })

  function handleRefreshClick(): void {
    fetchFSMState(false)
  }
</script>

<PageWithMenu>
  <div class="admin-container">
    <header class="admin-header">
      <h1>{t('admin.title', 'Admin Dashboard')}</h1>
      <div class="header-actions">
        <LogoutButton buttonClass="header-logout" />
      </div>
    </header>

    <div class="admin-section fsm-section">
      <div class="section-header">
        <h2>Blockchain FSM State Management</h2>
      </div>

      <div class="admin-card fsm-card">
        <div class="fsm-content-container">
          {#if fsmLoading && !fsmState}
            <div class="spinner-container">
              <div class="spinner"></div>
            </div>
          {:else if fsmError && !fsmState}
            <div class="error-container">
              <div class="error-message">
                <p><i class="fas fa-exclamation-triangle"></i> {fsmError}</p>
              </div>
              <button class="btn btn-primary" on:click={handleRefreshClick}>
                <i class="fas fa-sync-alt"></i> Retry
              </button>
            </div>
          {:else if fsmState}
            <div class="fsm-state-container">
              <div class="fsm-state-info">
                <h3>Current State</h3>
                <div
                  class="state-indicator"
                  style={`background-color: ${getStateColor(fsmState.state_value)}`}
                >
                  <h2 class="state-name">{fsmState.state}</h2>
                </div>
              </div>

              <div class="fsm-actions">
                <h3>Actions</h3>
                {#if fsmEvents.length === 0}
                  <p class="no-events">No actions available</p>
                {:else}
                  <div class="action-buttons">
                    {#each fsmEvents.sort((a, b) => a.value - b.value) as event}
                      {#if event && event.name}
                        {@const allowed = isEventAllowedForState(fsmState?.state, event.name)}
                        <button
                          on:click={() => sendFSMEvent(event.name)}
                          disabled={fsmLoading || !allowed}
                          class="action-button"
                          data-event={event.name.toLowerCase()}
                          data-event-id={event.value}
                          title={!allowed ? getEventDisabledReason(fsmState?.state, event.name) : ''}
                        >
                          {#if fsmLoading}
                            <div class="spinner"></div>
                          {:else}
                            <i class={getEventIcon(event.name)}></i>
                          {/if}
                          <span>{formatEventName(event.name)}</span>
                        </button>
                      {/if}
                    {/each}
                  </div>
                {/if}
              </div>
            </div>
          {:else}
            <div class="no-state">
              <p>No state information available.</p>
              <button class="btn btn-primary" on:click={handleRefreshClick}>
                <i class="fas fa-sync-alt"></i> Refresh
              </button>
            </div>
          {/if}
        </div>
      </div>
    </div>

    <!-- Block Invalidation Section -->
    <div class="admin-section">
      <div class="section-header">
        <h2>Invalidate Block</h2>
      </div>
      <div class="admin-card">
        <div class="block-operations">
          <p class="block-operations-description">
            Use this tool to invalidate or revalidate a specific block by its hash. Invalidating a
            block marks it as invalid in the blockchain, while revalidating reverses this action.
          </p>
          <div class="block-input-container">
            <label for="blockHash">Block Hash:</label>
            <input
              type="text"
              id="blockHash"
              bind:value={blockHash}
              placeholder="Enter block hash (64 hex characters)"
              class="block-hash-input"
              class:invalid={blockHash && !hashRegex.test(blockHash)}
            />
            {#if blockHash}
              <div class="hash-validation-message">
                {#if hashRegex.test(blockHash)}
                  <span class="valid-hash"
                    ><i class="fas fa-check-circle"></i> Valid hash format</span
                  >
                {:else}
                  <span class="invalid-hash">
                    <i class="fas fa-exclamation-circle"></i> Invalid hash format
                  </span>
                {/if}
              </div>
            {/if}
          </div>

          <div class="block-actions">
            <button
              class="block-action-button"
              on:click={() => performBlockAction(api.invalidateBlock, 'invalidate')}
              disabled={!hashRegex.test(blockHash) || blockActionLoading}
            >
              <i class="fas fa-ban"></i> Invalidate Block
            </button>

            <button
              class="block-action-button"
              on:click={() => performBlockAction(api.revalidateBlock, 'revalidate')}
              disabled={!hashRegex.test(blockHash) || blockActionLoading}
            >
              <i class="fas fa-check-circle"></i> Revalidate Block
            </button>
          </div>

          {#if blockActionLoading}
            <div class="block-operation-loading">
              <div class="spinner"></div>
            </div>
          {/if}
        </div>
      </div>
    </div>

    <!-- Invalid Blocks Section -->
    <div class="admin-section">
      <div class="section-header">
        <h2>Recently Invalidated Blocks</h2>
        <div class="refresh-container">
          <button
            class="icon-button with-text"
            on:click={goToPrevInvalidBlocksPage}
            disabled={invalidBlocksLoading || invalidBlocksOffset === 0}
            title="Previous page"
          >
            <span>Prev</span>
          </button>
          <span class="last-refresh">
            Showing {invalidBlocksShowingFrom}-{invalidBlocksShowingTo}
          </span>
          <button
            class="icon-button with-text"
            on:click={goToNextInvalidBlocksPage}
            disabled={invalidBlocksLoading || !invalidBlocksHasMore}
            title="Next page"
          >
            <span>Next</span>
          </button>
          {#if lastInvalidBlocksRefresh}
            <span class="last-refresh">
              Last refreshed: {lastInvalidBlocksRefresh.toLocaleTimeString()}
            </span>
          {/if}
          <button
            class="icon-button"
            on:click={() => fetchInvalidBlocks()}
            disabled={invalidBlocksLoading}
            title="Refresh invalidated blocks list"
          >
            <Icon
              name="icon-refresh-line"
              size={20}
              class={invalidBlocksLoading ? 'spinning' : ''}
            />
          </button>
        </div>
      </div>
      <div class="admin-card">
        <div class="invalid-blocks">
          {#if invalidBlocksLoading}
            <div class="invalid-blocks-loading">
              <div class="spinner-container">
                <div class="spinner"></div>
                <p>Loading invalidated blocks...</p>
              </div>
            </div>
          {:else if invalidBlocksError}
            <div class="invalid-blocks-error">
              <i class="fas fa-exclamation-circle"></i>
              <p>Error loading invalidated blocks</p>
              <p class="error-message">{invalidBlocksError}</p>
              <button class="icon-button with-text" on:click={() => fetchInvalidBlocks()}>
                <Icon name="icon-refresh-line" size={16} />
                <span>Try again</span>
              </button>
            </div>
          {:else if invalidBlocks.length === 0}
            <div class="invalid-blocks-empty">
              <i class="fas fa-check-circle"></i>
              <p>No invalidated blocks found</p>
              <p class="empty-description">All blocks in the blockchain are currently valid</p>
            </div>
          {:else}
            <div class="invalid-blocks-list">
              <table>
                <thead>
                  <tr>
                    <th style="width: 100px; text-align: center;">Height</th>
                    <th style="width: auto;">Hash</th>
                    <th style="width: 100px; text-align: center;">Size</th>
                    <th style="width: 150px; text-align: center;">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {#each invalidBlocks as block}
                    <tr>
                      <td class="height-cell">{block.height}</td>
                      <td class="hash-cell">
                        <RenderHashWithMiner
                          hash={block.hash}
                          hashUrl={`/viewer/block?q=${block.hash}`}
                          shortHash={`${block.hash.substring(0, 8)}...${block.hash.substring(block.hash.length - 8)}`}
                          showCopyButton={true}
                          copyTooltip="Copy hash to clipboard"
                          tooltip={block.hash}
                        />
                      </td>
                      <td style="text-align: center;">
                        {block.size ? `${block.size} B` : '-'}
                      </td>
                      <td class="actions-cell">
                        <button
                          class="revalidate-button"
                          on:click={() => revalidateBlock(block.hash)}
                          disabled={revalidatingBlock}
                        >
                          {#if revalidatingBlock && revalidatingBlockHash === block.hash}
                            <div class="button-spinner"></div>
                            <span>Re-validating...</span>
                          {:else}
                            <i class="fas fa-sync-alt"></i>
                            <span>Re-validate</span>
                          {/if}
                        </button>
                      </td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}
        </div>
      </div>
    </div>

    <!-- Reset Peer Reputations Section -->
    <div class="admin-section">
      <div class="section-header">
        <h2>Reset Peer Reputations</h2>
      </div>
      <div class="admin-card">
        <p class="section-description">
          Reset reputation scores for all peers in the peer registry. This will clear all interaction metrics and set reputation scores back to neutral (50.0).
        </p>
        <div class="action-buttons">
          <button
            class="action-button neutral"
            on:click={resetPeerReputations}
            disabled={resettingReputations}
          >
            {#if resettingReputations}
              <i class="fas fa-spinner fa-spin"></i>
              <span>Resetting...</span>
            {:else}
              <i class="fas fa-undo"></i>
              <span>Reset All Peer Reputations</span>
            {/if}
          </button>
        </div>
      </div>
    </div>
  </div>
</PageWithMenu>

<style>
  .admin-container {
    padding: 2rem;
    max-width: 1200px;
    margin: 0 auto;
    color: #e9ecef;
  }

  .admin-section {
    margin-bottom: 3rem;
    background-color: rgba(33, 37, 41, 0.6);
    border-radius: 0.75rem;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
    overflow: hidden;
  }

  .section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 0;
    padding: 1.25rem 1.5rem;
    background-color: rgba(33, 37, 41, 0.8);
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  }

  .section-header h2 {
    font-size: 1.5rem;
    font-weight: 600;
    color: #f8f9fa;
    margin: 0;
    letter-spacing: 0.01em;
  }

  .admin-card {
    background-color: #111827;
    border-radius: 0.5rem;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
    box-shadow:
      0 4px 6px -1px rgba(0, 0, 0, 0.1),
      0 2px 4px -1px rgba(0, 0, 0, 0.06);
  }

  .fsm-section {
    margin-bottom: 3rem;
  }

  .fsm-card {
    padding: 0;
  }

  .fsm-content-container {
    min-height: 200px;
    position: relative;
  }

  .fsm-state-container {
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: stretch;
    min-height: 200px;
    background-color: rgba(30, 30, 30, 0.5);
    border-radius: 0.75rem;
    padding: 1rem;
  }

  .spinner-container {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    display: flex;
    justify-content: center;
    align-items: center;
    background-color: rgba(20, 20, 20, 0.2);
    border-radius: 0.75rem;
    min-height: 200px;
  }

  .fsm-state-info {
    padding: 1.5rem 2rem;
    flex: 1;
    border-right: 1px solid rgba(255, 255, 255, 0.1);
    display: flex;
    flex-direction: column;
  }

  .fsm-state-info h3 {
    font-size: 1.25rem;
    font-weight: 500;
    margin-bottom: 1.25rem;
    color: #ced4da;
  }

  .state-indicator {
    padding: 2rem;
    border-radius: 0.75rem;
    text-align: center;
    transition: all 0.3s ease;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    position: relative;
    overflow: hidden;
    flex: 1;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
  }

  .state-indicator::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    background: rgba(255, 255, 255, 0.3);
  }

  .state-name {
    font-size: 2.25rem;
    font-weight: 700;
    margin: 0;
    color: #fff;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
    letter-spacing: 0.02em;
  }

  .fsm-actions {
    padding: 1.5rem 2rem;
    flex: 1;
    display: flex;
    flex-direction: column;
  }

  .fsm-actions h3 {
    font-size: 1.25rem;
    font-weight: 500;
    margin-bottom: 1.25rem;
    color: #ced4da;
  }

  .action-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    flex: 1;
    align-content: flex-start;
  }

  .action-button {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
    padding: 0.875rem 1.5rem;
    border-radius: 0.5rem;
    font-size: 1rem;
    font-weight: 600;
    border: none;
    cursor: pointer;
    transition: all 0.2s ease;
    color: white;
    background-color: #3b82f6;
    min-width: 140px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    position: relative;
  }

  .action-button :global(.spinner-small) {
    width: 16px;
    height: 16px;
  }

  .action-button:hover {
    background-color: #2563eb;
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
  }

  .action-button:active {
    transform: translateY(0);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .action-button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
    transform: none;
    box-shadow: none;
  }

  .action-button[data-event='stop'] {
    background-color: #ef4444;
  }

  .action-button[data-event='stop']:hover {
    background-color: #dc2626;
  }

  .action-button[data-event='run'] {
    background-color: #10b981;
  }

  .action-button[data-event='run']:hover {
    background-color: #059669;
  }

  .action-button[data-event='catchup'] {
    background-color: #6366f1;
  }

  .action-button[data-event='catchup']:hover {
    background-color: #4f46e5;
  }

  .action-button[data-event='legacysync'] {
    background-color: #8b5cf6;
  }

  .action-button[data-event='legacysync']:hover {
    background-color: #7c3aed;
  }

  .action-button.neutral {
    background-color: #6c757d;
  }

  .action-button.neutral:hover {
    background-color: #5a6268;
  }

  .action-button i {
    font-size: 1.125rem;
  }

  .error-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1.5rem;
    padding: 2.5rem 2rem;
    text-align: center;
  }

  .error-message {
    color: #ef4444;
    font-size: 1.125rem;
    background-color: rgba(239, 68, 68, 0.1);
    border: 1px solid rgba(239, 68, 68, 0.2);
    padding: 1rem 1.5rem;
    border-radius: 0.5rem;
    width: 100%;
    max-width: 500px;
  }

  .error-message i {
    margin-right: 0.5rem;
    font-size: 1.25rem;
  }

  .no-events {
    color: #9ca3af;
    font-style: italic;
    padding: 1rem;
    text-align: center;
    background-color: rgba(156, 163, 175, 0.1);
    border-radius: 0.5rem;
  }

  .no-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 1.5rem;
    padding: 3rem 2rem;
    text-align: center;
    color: #9ca3af;
  }

  .btn {
    padding: 0.75rem 1.5rem;
    border-radius: 0.5rem;
    font-weight: 600;
    font-size: 1rem;
    border: none;
    cursor: pointer;
    transition: all 0.2s ease;
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
  }

  .btn-primary {
    background-color: #3b82f6;
    color: white;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .btn-primary:hover {
    background-color: #2563eb;
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
  }

  .btn-primary:active {
    transform: translateY(0);
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .hash-validation-message {
    font-size: 0.875rem;
    margin-top: 0.5rem;
    padding: 0.5rem;
    border-radius: 0.375rem;
  }

  .valid-hash {
    color: #10b981;
    display: flex;
    align-items: center;
    gap: 0.375rem;
  }

  .invalid-hash {
    color: #ef4444;
    display: flex;
    align-items: center;
    gap: 0.375rem;
  }

  .block-operation-loading {
    display: flex;
    justify-content: center;
    min-height: 100px;
  }

  .block-operation-result {
    padding: 1rem 1.5rem;
    border-radius: 0.5rem;
    margin-top: 1.5rem;
    display: flex;
    align-items: center;
    gap: 0.75rem;
  }

  .block-operation-result.success {
    background-color: rgba(46, 204, 113, 0.1);
    color: #2ecc71;
    border: 1px solid rgba(46, 204, 113, 0.3);
  }

  .block-operation-result.error {
    background-color: rgba(231, 76, 60, 0.1);
    color: #e74c3c;
    border: 1px solid rgba(231, 76, 60, 0.3);
  }

  .admin-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 2.5rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  }

  .admin-header h1 {
    font-size: 2rem;
    font-weight: 700;
    color: #f8f9fa;
    margin: 0;
    letter-spacing: 0.01em;
  }

  .header-actions {
    display: flex;
    gap: 1rem;
  }

  .header-logout {
    font-size: 0.8rem;
    padding: 0.4rem 0.8rem;
  }

  .logout-button {
    background-color: rgba(107, 114, 128, 0.1);
    color: #e5e7eb;
    border: 1px solid rgba(209, 213, 219, 0.2);
    border-radius: 0.5rem;
    padding: 0.625rem 1.25rem;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    transition: all 0.2s;
  }

  .logout-button:hover {
    background-color: rgba(107, 114, 128, 0.2);
    color: #f3f4f6;
    border-color: rgba(209, 213, 219, 0.3);
  }

  .block-operations {
    padding: 1.5rem;
  }

  .block-operations-description {
    margin-bottom: 1.5rem;
    color: #ced4da;
    line-height: 1.6;
    font-size: 1rem;
  }

  .block-input-container {
    margin-bottom: 1.5rem;
  }

  .block-input-container label {
    display: block;
    margin-bottom: 0.5rem;
    font-weight: 500;
    color: #e9ecef;
  }

  .block-hash-input {
    padding: 0.75rem 1rem;
    border-radius: 0.5rem;
    font-size: 1rem;
    border: 1px solid #4b5563;
    background-color: rgba(30, 30, 30, 0.6);
    color: #e9ecef;
    width: 100%;
    transition: all 0.2s ease;
  }

  .block-hash-input:focus {
    outline: none;
    border-color: #3b82f6;
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
  }

  .block-hash-input.invalid {
    border-color: #ef4444;
    box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.2);
  }

  .block-actions {
    display: flex;
    justify-content: flex-end;
    margin-top: 1rem;
    padding-top: 0.75rem;
    border-top: 1px solid rgba(255, 255, 255, 0.1);
  }

  .text-button {
    background: none;
    border: none;
    color: #3b82f6;
    cursor: pointer;
    padding: 0.25rem 0.5rem;
    font-size: 0.875rem;
    text-decoration: underline;
  }

  .text-button:hover {
    color: #2563eb;
  }

  .invalid-blocks {
    padding: 0;
  }

  .invalid-blocks-list {
    max-height: 600px;
    overflow-y: auto;
    padding: 0;
    width: 100%;
    background-color: #111827;
    border-radius: 0.5rem;
  }

  .invalid-blocks-list::-webkit-scrollbar {
    width: 8px;
  }

  .invalid-blocks-list::-webkit-scrollbar-track {
    background: rgba(255, 255, 255, 0.1);
    border-radius: 4px;
  }

  .invalid-blocks-list::-webkit-scrollbar-thumb {
    background-color: rgba(255, 255, 255, 0.2);
    border-radius: 4px;
  }

  .invalid-blocks-list::-webkit-scrollbar-thumb:hover {
    background-color: rgba(255, 255, 255, 0.3);
  }

  .invalid-blocks-list table {
    width: 100%;
    border-collapse: collapse;
    color: #e5e7eb;
    font-size: 0.9rem;
    table-layout: fixed;
    border-spacing: 0;
  }

  .invalid-blocks-list th {
    text-align: left;
    padding: 0.75rem 1rem;
    font-weight: 600;
    color: var(--table-th-color, #232d7c);
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    white-space: nowrap;
    font-size: 0.85rem;
    /* Removed text-transform: uppercase; */
    letter-spacing: 0.02em;
  }

  .invalid-blocks-list td {
    padding: 0.75rem 1rem;
    border-bottom: 1px solid var(--table-td-border-bottom, 1px solid #fcfcff);
    border-top: var(--table-td-border-top, 1px solid #efefef);
    vertical-align: middle;
    color: var(--table-td-color, #282933);
  }

  /* Removed hover effect for table rows */
  .invalid-blocks-list tr:hover {
    background-color: transparent;
  }

  .height-cell {
    color: #3b82f6;
    font-weight: 500;
    width: 100px;
    text-align: center;
  }

  .hash-cell {
    width: auto;
    overflow: hidden;
  }

  .actions-cell {
    display: flex;
    justify-content: center;
    width: 150px;
  }

  .invalid-blocks-loading {
    display: flex;
    justify-content: center;
    align-items: center;
    padding: 2rem;
  }

  .invalid-blocks-error {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    text-align: center;
  }

  .invalid-blocks-error i {
    font-size: 3rem;
    color: #ef4444;
    margin-bottom: 1rem;
  }

  .error-message {
    font-weight: normal !important;
    color: #6b7280 !important;
    font-size: 0.9rem;
    max-width: 80%;
    word-break: break-word;
    margin-bottom: 1rem !important;
  }

  .invalid-blocks-empty {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    text-align: center;
  }

  .invalid-blocks-empty i {
    font-size: 3rem;
    color: #10b981;
    margin-bottom: 1rem;
  }

  .invalid-blocks-empty p {
    margin: 0.25rem 0;
    font-weight: 600;
    color: #4b5563;
  }

  .empty-description {
    font-weight: normal !important;
    color: #6b7280 !important;
    font-size: 0.9rem;
  }

  .copy-icon {
    font-size: 16px;
    line-height: 1;
    font-weight: bold;
  }

  .revalidate-button {
    background-color: #3b82f6;
    color: white;
    border: none;
    transition: background-color 0.2s ease;
    padding: 0.5rem 0.75rem;
    border-radius: 0.375rem;
    font-size: 0.875rem;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }

  .revalidate-button:hover {
    background-color: #2563eb;
  }

  .revalidate-button:disabled {
    background-color: #6b7280;
    cursor: not-allowed;
  }

  .button-spinner {
    width: 1rem;
    height: 1rem;
    border: 2px solid rgba(255, 255, 255, 0.3);
    border-top-color: white;
    border-radius: 50%;
    animation: spin 1s linear infinite;
  }

  .detail-label {
    font-weight: 600;
    color: #1f2937;
    margin-right: 0.25rem;
  }

  .timestamp {
    color: #6b7280;
  }

  .reason {
    color: #6b7280;
  }

  .reason-text {
    color: #4b5563;
  }

  /* Add responsive behavior for smaller screens */
  @media (max-width: 768px) {
    .fsm-state-container {
      flex-direction: column;
    }

    .fsm-state-info {
      border-right: none;
      border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
  }

  .refresh-button {
    background-color: #f3f4f6;
    border: 1px solid #e5e7eb;
    color: #4b5563;
    cursor: pointer;
    padding: 0.5rem 0.75rem;
    border-radius: 0.375rem;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.875rem;
    font-weight: 500;
  }

  .refresh-button:hover {
    color: #3b82f6;
    background-color: rgba(59, 130, 246, 0.1);
    border-color: #3b82f6;
  }

  .refresh-button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }

  .spinning {
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }

  .block-action-button {
    padding: 0.75rem 1.25rem;
    border-radius: 0.5rem;
    font-size: 1rem;
    font-weight: 600;
    border: none;
    cursor: pointer;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    min-width: 180px;
    justify-content: center;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    background-color: #3b82f6;
    color: white;
  }

  .block-action-button i {
    font-size: 1.125rem;
  }

  .block-action-button:hover {
    background-color: #2563eb;
    transform: translateY(-2px);
    box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
  }

  .block-action-button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
    transform: none;
    box-shadow: none;
  }

  .invalid-blocks-loading {
    display: flex;
    justify-content: center;
    padding: 2rem;
  }

  .spinner {
    border: 4px solid rgba(0, 0, 0, 0.1);
    width: 36px;
    height: 36px;
    border-radius: 50%;
    border-left-color: #3b82f6;
    animation: spin 1s linear infinite;
  }

  .spinner-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
  }

  .spinner-container p {
    margin-top: 1rem;
    color: #6b7280;
    font-size: 0.9rem;
  }

  .refresh-container {
    display: flex;
    align-items: center;
    gap: 1rem;
  }

  .last-refresh {
    font-size: 0.875rem;
    color: #6b7280;
    white-space: nowrap;
  }

  .icon-button {
    background-color: transparent;
    border: none;
    color: #6b7280;
    cursor: pointer;
    padding: 0.5rem;
    border-radius: 0.25rem;
    transition: all 0.2s ease;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .icon-button.with-text {
    gap: 0.5rem;
    padding: 0.5rem 0.75rem;
  }

  .icon-button:hover {
    color: #3b82f6;
    background-color: rgba(59, 130, 246, 0.1);
  }

  .icon-button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
</style>
