<script lang="ts">
  import { onMount } from 'svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import ConnectedNodesCard from '$internal/components/page/network/connected-nodes-card/index.svelte'

  import { miningNodes, sock } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  let nodes: any[] = []

  function updateData() {
    const mNodes: any[] = []
    // console.log($miningNodes)
    Object.values($miningNodes).forEach((node) => {
      mNodes.push(node)
    })
    const sorted = mNodes.sort((a: any, b: any) => {
      if (a.base_url < b.base_url) {
        return -1
      } else if (a.base_url > b.base_url) {
        return 1
      } else {
        return 0
      }
    })

    nodes = sorted
  }

  onMount(() => {
    updateData()

    const interval = setInterval(() => {
      updateData()
    }, 1000)

    return () => clearInterval(interval)
  })
</script>

<PageWithMenu>
  <ConnectedNodesCard data={nodes} {connected} />
</PageWithMenu>

<style>
</style>
