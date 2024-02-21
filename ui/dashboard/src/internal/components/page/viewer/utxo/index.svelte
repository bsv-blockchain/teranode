<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'

  import NoData from '../no-data-card/index.svelte'
  import { DetailTab, DetailType } from '$internal/utils/urls'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  const type = DetailType.utxo

  export let hash = ''

  let display: DetailTab

  $: tab = ready ? $page.url.searchParams.get('tab') ?? '' : ''
  $: display = tab === DetailTab.json ? DetailTab.json : DetailTab.overview

  let result: any = null

  $: {
    if ($assetHTTPAddress && type && hash && hash.length === 64) {
      fetchData()
    }
  }

  async function fetchData() {
    result = null
    //
  }
</script>

{#if result}
  <div>todo</div>
{:else if $spinCount === 0}
  <div class="no-data">
    <NoData {hash} />
  </div>
{/if}

<style>
  .no-data {
    padding-top: 80px;
  }
</style>
