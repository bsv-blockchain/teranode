<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'

  import BlockDetails from '$internal/components/page/viewer/block/index.svelte'
  import SubtreeDetails from '$internal/components/page/viewer/subtree/index.svelte'
  import TxDetails from '$internal/components/page/viewer/tx/index.svelte'
  import UtxoDetails from '$internal/components/page/viewer/utxo/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { DetailType } from '$internal/utils/urls'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  $: type = $page.params.type
  $: hash = ready ? $page.url.searchParams.get('hash') ?? '' : ''
</script>

<PageWithMenu>
  {#if type === DetailType.block}
    <BlockDetails {hash} />
  {:else if type === DetailType.subtree}
    <SubtreeDetails {hash} />
  {:else if type === DetailType.tx}
    <TxDetails {hash} />
  {:else if type === DetailType.utxo}
    <UtxoDetails {hash} />
  {/if}
</PageWithMenu>

<style>
</style>
