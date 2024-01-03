<script lang="ts">
  import { onMount } from 'svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import ConnectedNodesCard from '$internal/components/page/network/connected-nodes-card/index.svelte'

  import { miningNodes, sock } from '$internal/stores/p2pStore'
  import { humanTime } from '$internal/utils/humanTime'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  let nodes: any[] = []

  function updateData() {
    const tmp: any[] = []
    $miningNodes.forEach((node) => {
      tmp.push({
        ...node,
        receivedAt: humanTime(node.receivedAt),
      })
    })
    nodes = tmp
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
