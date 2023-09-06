<script>
  import { localMode } from '@stores/nodeStore.js'

  export let size = '20px' // The default is 20px
  export let updateFunc = null
  export let spin = false
  export let text = 0
</script>

{#if spin}
  <div class="overlay">
    <div class="spinner" style="--spinner-size: {size};" />
  </div>
{:else}
  <div
    class="overlay"
    title="When enabled the app will use the BlobServer at localhost:8099"
  >
    <label class="checkbox">
      <input type="checkbox" bind:checked={$localMode} />
      <span class="label2">Local Mode</span>
    </label>

    {#if updateFunc}
      <button
        class="spinner-button"
        style="--spinner-size: {size};"
        on:click={updateFunc}
      >
        {text === 0 ? '' : text}
      </button>
    {:else}
      <button class="spinner-button" style="--spinner-size: {size};">
        {text === 0 ? '' : text}
      </button>
    {/if}
  </div>
{/if}

<style>
  .overlay {
    position: relative;
    display: flex; /* Use flexbox */
    align-items: center; /* Align items vertically in the center */
    justify-content: center; /* Align items horizontally in the center */
  }

  .spinner {
    border: 4px solid rgba(255, 255, 255, 0.1);
    border-radius: 50%;
    border-top: 4px solid white;
    width: var(--spinner-size, 20px);
    height: var(--spinner-size, 20px);
    animation: spin 1s linear infinite;
    margin: 10px; /* Adjust margin as needed */
  }

  .spinner-button {
    border: 4px solid white;
    background-color: white;
    border-radius: 50%;
    /* border-top: 4px solid white; Thinner top border */
    width: var(--spinner-size, 20px);
    height: var(--spinner-size, 20px);
    margin: 10px; /* Adjust margin as needed */
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .label {
    color: white;
  }

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }
</style>
