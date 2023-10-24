<script>
  import { blobServerHTTPAddress } from '@stores/nodeStore.js'
  import { goto } from '$app/navigation'
  import JSONTree from '@components/JSONTree.svelte'
  import Spinner from '@components/Spinner.svelte'

  export let data

  let loading = false
  let res = null
  let error = null
  let url = ''

  $: if ($blobServerHTTPAddress && data.hash && data.hash.length === 64) {
    url = $blobServerHTTPAddress + '/utxos/' + data.hash + '/json'
    fetchData()
  } else {
    url = ''
  }

  async function fetchData() {
    if (!url) return

    try {
      error = null
      loading = true

      const response = await fetch(url)
      if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`)
      }

      const d = await response.json()

      res = d

      goto(`/utxoviewer/${data.hash}`, { replaceState: true })
    } catch (err) {
      error = err.message
    } finally {
      loading = false
    }
  }
</script>

{#if loading}
  <Spinner />
{/if}
<section class="section">
  <section class="search-bar">
    <div class="search-field">
      <input
        class="input search-input"
        type="text"
        bind:value={data.hash}
        placeholder="Enter transaction id / hash"
        maxlength="64"
      />
    </div>

    <div class="search-field">
      <button class="button is-info" on:click={fetchData} disabled={url === ''}
        >Search</button
      >
    </div>
  </section>

  <div class="url">{url}</div>

  <div class="result">
    {#if error}
      <div class="error-message">{error}</div>
    {:else if res}
      <div class="data-box">
        <JSONTree data={res} />
      </div>
    {/if}
  </div>
</section>

<style>
  .search-bar {
    display: flex;
    align-items: flex-start;
    justify-content: flex-start;
    flex-direction: row;
    padding: 20px;
  }

  .search-field {
    position: relative;
    display: inline-block;
  }

  .search-input {
    width: 650px;
    margin-right: 10px;
    padding: 8px;
    border: 1px solid #ccc;
    border-radius: 4px;
  }

  .result {
    margin-top: 20px;
  }

  .error-message {
    color: red;
  }

  .data-box {
    border: 1px solid #ccc;
    border-radius: 4px;
    padding: 10px;
    max-width: 100%;
    overflow: auto;
  }

  .url {
    margin-left: 25px;
    padding: 10px;
    font-size: 0.7rem;
  }
</style>
