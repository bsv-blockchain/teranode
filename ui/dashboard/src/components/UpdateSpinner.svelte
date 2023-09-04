<script>
  export let size = '20px' // The default is 20px
  export let updateFunc
  export let spin = false
  export let text = 0
</script>

{#if spin}
  <div class="overlay">
    <div class="spinner" style="--spinner-size: {size};" />
  </div>
{:else}
  <div class="overlay">
    {#if updateFunc}
      <button
        class="spinner-button"
        style="--spinner-size: {size};"
        on:click={updateFunc}
      >
        {text === 0 ? '' : text}
      </button>
    {:else}
      <div class="spinner-button" style="--spinner-size: {size};">
        {text === 0 ? '' : text}
      </div>
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

  @keyframes spin {
    0% {
      transform: rotate(0deg);
    }
    100% {
      transform: rotate(360deg);
    }
  }
</style>
