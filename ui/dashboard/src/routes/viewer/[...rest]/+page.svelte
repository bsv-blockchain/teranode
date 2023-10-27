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

  $: if (
    $blobServerHTTPAddress &&
    data.type &&
    data.hash &&
    data.hash.length === 64
  ) {
    url = $blobServerHTTPAddress + '/' + data.type + '/' + data.hash + '/json'
    fetchData()
  } else {
    url = ''
  }

  const itemTypes = [
    'block',
    'header',
    'subtree',
    'tx',
    'utxo',
    'utxos',
    'txmeta',
  ]

  function reverse() {
    if (data && data.hash && data.hash.length === 64) {
      const hexString = data.hash
      const byteLength = hexString.length / 2
      const byteArray = new Uint8Array(byteLength)

      for (let i = 0; i < byteLength; i++) {
        const byte = parseInt(hexString.substr(i * 2, 2), 16)
        byteArray[i] = byte
      }

      byteArray.reverse()

      let reversedHexString = ''
      for (let i = 0; i < byteArray.length; i++) {
        reversedHexString += byteArray[i].toString(16).padStart(2, '0')
      }

      data.hash = reversedHexString

      data = { ...data }
    }
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

      goto(`/viewer/${data.type}/${data.hash}`, { replaceState: true })
    } catch (err) {
      error = err.message
      console.error(err)
    } finally {
      loading = false
    }
  }
</script>

{#if loading}
  <Spinner />
{/if}
<section class="section">
  <!-- Dropdown for URL selection -->

  <section class="search-bar">
    <button class="reverse" on:click={reverse}>Reverse</button>
    <div class="search-field">
      <div class="control">
        <div class="select">
          <select bind:value={data.type}>
            <option value="">-- Select Type --</option>
            {#each itemTypes as type}
              <option value={type}>{type}</option>
            {/each}
          </select>
        </div>
      </div>
    </div>

    <div class="search-field">
      <input
        class="input search-input"
        type="text"
        bind:value={data.hash}
        placeholder="Enter hash"
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

  .reverse {
    position: relative;
    top: -20px; /* Adjust the top position as needed */
    left: 300px;
    right: 0;
    z-index: 1; /* Ensure it's above other elements */
  }

  .search-input {
    flex-grow: 1; /* Allow the input to take up remaining space */
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
