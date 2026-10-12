<svelte:options runes={true} />

<script lang="ts">
  // import { page } from '$app/stores'
  import UtxoDetailsCard from './utxo-details-card/index.svelte'

  import NoData from '../no-data-card/index.svelte'
  import { DetailType } from '$internal/utils/urls'
  import { spinCount } from '$internal/stores/nav'
  import { assetHTTPAddress } from '$internal/stores/nodeStore'

  const type = DetailType.utxo

  let { hash = '' }: { hash?: string } = $props()

  let result: any = $state(null)

  $effect(() => {
    if ($assetHTTPAddress && type && hash && hash.length === 64) {
      fetchData()
    }
  })

  async function fetchData() {
    result = { hash }
    //
  }
</script>

{#if result}
  <UtxoDetailsCard data={result} />
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
