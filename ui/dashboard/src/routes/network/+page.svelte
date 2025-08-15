<script lang="ts">
  import { onMount } from 'svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import ConnectedNodesCard from '$internal/components/page/network/connected-nodes-card/index.svelte'
  import { calculateChainworkScores } from '$internal/components/page/network/connected-nodes-card/data'

  import { miningNodes, sock, currentNodePeerID } from '$internal/stores/p2pStore'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  $: connected = $sock !== null

  let nodes: any[] = []

  function updateData() {
    const mNodes: any[] = []
    Object.values($miningNodes).forEach((node) => {
      mNodes.push(node)
    })

    // Calculate chainwork scores
    const chainworkScores = calculateChainworkScores(mNodes)
    const maxScore = Math.max(...Array.from(chainworkScores.values()))
    
    // Get current node peer ID
    const currentPeerID = $currentNodePeerID

    // Add scores to each node and update isCurrentNode flag
    mNodes.forEach((node) => {
      const key = node.peer_id
      node.chainwork_score = chainworkScores.get(key) || 0
      node.maxChainworkScore = maxScore
      // Update isCurrentNode based on reactive store
      node.isCurrentNode = node.peer_id === currentPeerID
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
