<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import BlockDetails from '$internal/components/page/viewer/block/index.svelte'
  import SubtreeDetails from '$internal/components/page/viewer/subtree/index.svelte'
  import TxDetails from '$internal/components/page/viewer/tx/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { page } from '$app/stores'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  $: type = $page.params.type
  $: hash = ready ? $page.url.searchParams.get('hash') ?? '' : ''
</script>

<PageWithMenu>
  {#if type === 'block'}
    <BlockDetails {hash} />
  {:else if type === 'subtree'}
    <SubtreeDetails {hash} />
  {:else if type === 'tx'}
    <TxDetails {hash} />
  {/if}
</PageWithMenu>

<style>
</style>
