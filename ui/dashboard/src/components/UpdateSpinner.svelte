<script>
  export let size = '20px' // The default is 20px
  export let updateFunc = null
  export let flash = false
  export let text = 0

  $: updateFunction = updateFunc || function () {}
</script>

<div
  class="overlay"
  title="When enabled the app will use the BlobServer at localhost:8099"
>
  <button
    class="spinner-button"
    style="--spinner-size: {size};"
    on:click={updateFunction}
  >
    {#if flash}
      <div class="led" />
    {:else}
      {text === 0 ? '' : text}
    {/if}
  </button>
</div>

<style>
  .overlay {
    position: relative;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .led {
    background-color: red; /* Red color for LED */
    border-radius: 50%;
    min-width: var(--led-size, 10px);
    min-height: var(--led-size, 10px);
    width: var(--led-size, 10px);
    height: var(--led-size, 10px);
    animation: flash 0.5s linear infinite alternate; /* Flashing animation */
    margin: 10px;
    z-index: 1;
  }

  .spinner-button {
    border: 4px solid white;
    background-color: white;
    border-radius: 50%;
    width: var(--spinner-size, 20px);
    height: var(--spinner-size, 20px);
    margin: 10px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  @keyframes flash {
    0% {
      opacity: 1;
    }
    100% {
      opacity: 0;
    }
  }
</style>
