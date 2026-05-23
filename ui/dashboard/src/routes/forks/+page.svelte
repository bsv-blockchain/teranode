<script lang="ts">
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { onMount } from 'svelte'

  import { treeBoxes } from './helpers'
  import * as api from '$internal/api'
  import { page } from '$app/stores'
  import { goto } from '$app/navigation'
  import { Button } from '$lib/components'

  let vis: HTMLDivElement
  let tree: any
  let pageSize = 20
  let mounted = false
  let lastLoadedHash = ''

  let nearestForks: {
    current_height: number
    prev_fork: { height: number; parent_hash: string } | null
    next_fork: { height: number; parent_hash: string } | null
  } | null = null

  $: hash = $page.url.searchParams.get('hash') || ''
  $: orientation = $page.url.searchParams.get('orientation') || checkOrientation()

  $: if (mounted && hash && hash !== lastLoadedHash) {
    loadData(hash)
  }

  function checkOrientation() {
    let orientation = 'left-to-right'

    if (window.matchMedia('(orientation: portrait)').matches) {
      orientation = 'top-to-bottom'
    } else if (window.matchMedia('(orientation: landscape)').matches) {
      orientation = 'left-to-right'
    }

    return orientation
  }

  async function loadData(h: string) {
    lastLoadedHash = h
    nearestForks = null
    tree = null
    if (vis) vis.innerHTML = ''
    await Promise.all([loadForkTree(h), loadNearestForks(h)])
  }

  async function loadForkTree(h: string) {
    try {
      const result: any = await api.getBlockForks({
        hash: h,
        limit: pageSize,
      })
      if (result.ok) {
        tree = result.data.tree
        redraw()
      }
    } catch (e) {
      console.error(e)
    }
  }

  async function loadNearestForks(h: string) {
    try {
      const result: any = await api.getNearestForkHeights({ hash: h })
      if (result.ok) {
        nearestForks = result.data
      }
    } catch (e) {
      console.error(e)
    }
  }

  function redraw() {
    if (vis) {
      vis.innerHTML = ''
    }
    treeBoxes(vis, tree, orientation)
  }

  function goToFork(blockHash: string) {
    goto(`/forks/?hash=${blockHash}`)
  }

  onMount(() => {
    mounted = true
    if (hash) {
      loadData(hash)
    }
  })
</script>

<PageWithMenu>
  <div class="content">
    <div class="fork-nav">
      <Button
        size="small"
        disabled={!nearestForks?.prev_fork}
        on:click={() => nearestForks?.prev_fork && goToFork(nearestForks.prev_fork.parent_hash)}
      >
        &larr; Prev fork{nearestForks?.prev_fork ? ` (h: ${nearestForks.prev_fork.height})` : ''}
      </Button>

      <div class="fork-info">
        {#if nearestForks}
          Height {nearestForks.current_height}
        {/if}
      </div>

      <Button
        size="small"
        disabled={!nearestForks?.next_fork}
        on:click={() => nearestForks?.next_fork && goToFork(nearestForks.next_fork.parent_hash)}
      >
        Next fork{nearestForks?.next_fork ? ` (h: ${nearestForks.next_fork.height})` : ''} &rarr;
      </Button>
    </div>
    <div id="vis" bind:this={vis}></div>
  </div>
</PageWithMenu>

<style>
  .content {
    width: 100%;
    max-width: 100%;
    overflow: scroll;

    display: flex;
    flex-direction: column;
    gap: 20px;
  }

  .fork-nav {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 20px;
    gap: 16px;
  }

  .fork-info {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14px;
    color: var(--app-color);
  }

  #vis {
    left: 0;
    width: 100%;
    overflow: scroll;
    padding: 20px;
  }

  :global(.svgContainer) {
    display: block;
    margin: auto;
  }

  :global(.node) {
    fill: #1878ff;
    cursor: pointer;
  }

  :global(.node-rect) {
  }

  :global(.node-rect-closed) {
    stroke-width: 2px;
    stroke: rgb(0, 0, 0);
  }

  :global(.link) {
    fill: none;
    stroke: lightsteelblue;
    stroke-width: 2px;
  }

  :global(.linkselected) {
    fill: none;
    stroke: tomato;
    stroke-width: 2px;
  }

  :global(.arrow) {
    fill: lightsteelblue;
    stroke-width: 1px;
  }

  :global(.arrowselected) {
    fill: tomato;
    stroke-width: 2px;
  }

  :global(.link text) {
    font: 7px sans-serif;
    fill: #cc0000;
  }

  :global(.wordwrap) {
    white-space: pre-wrap; /* CSS3 */
    white-space: -moz-pre-wrap; /* Firefox */
    white-space: -pre-wrap; /* Opera <7 */
    white-space: -o-pre-wrap; /* Opera 7 */
    word-wrap: break-word; /* IE */
  }

  :global(.node-text) {
    font: 10px sans-serif;
    font-weight: normal;
    color: #ddd;
  }

  :global(.tooltip-text-container) {
    height: 100%;
    width: 100%;
  }

  :global(.tooltip-text) {
    visibility: hidden;
    font: 7px sans-serif;
    color: white;
    display: block;
    padding: 5px;
  }

  :global(.tooltip-box) {
    background: rgba(0, 0, 0, 0.7);
    visibility: hidden;
    position: absolute;
    border-style: solid;
    border-width: 1px;
    border-color: black;
    border-top-right-radius: 0.5em;
  }

  :global(.textcolored) {
    color: orange;
  }

  :global(a.exchangeName) {
    color: orange;
  }
</style>
