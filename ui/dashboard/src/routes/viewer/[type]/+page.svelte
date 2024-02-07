<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import BlockDetails from '$internal/components/page/viewer/block/index.svelte'
  import SubtreeDetails from '$internal/components/page/viewer/subtree/index.svelte'
  import TxDetails from '$internal/components/page/viewer/tx/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { page } from '$app/stores'

  let ready = false

  $: type = $page.params.type
  $: hash = ready ? new URLSearchParams($page.url.search).get('hash') || '' : ''

  beforeUpdate(() => {
    ready = true
  })
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
