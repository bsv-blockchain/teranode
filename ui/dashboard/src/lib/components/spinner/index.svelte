<script lang="ts">
  import { Modal } from '$lib/components'

  export let size = 75
  export let speed = 550
  export let color = '#232d7c'
  export let coverColor = 'rgba(255, 255, 255, 0.7)'
  export let thickness = 1
  export let gap = 25
  export let radius = 10
  export let offsetX = 0

  // Handle predefined sizes
  if (size === 'small') {
    size = 16
    thickness = 2
    radius = 6
    gap = 20
  }

  let dash
  let marginLeft = 0
  $: {
    dash = (2 * Math.PI * radius * (100 - gap)) / 100
    marginLeft = offsetX
  }
</script>

<Modal flyContent={false} coverCol={coverColor}>
  <svg
    height={size}
    width={size}
    style="animation-duration:{speed}ms;"
    class="svelte-spinner {size === 16 ? 'spinner-small' : ''}"
    viewBox="0 0 32 32"
    style:--margin-left={marginLeft + 'px'}
  >
    <circle
      role="presentation"
      cx="16"
      cy="16"
      r={radius}
      stroke={color}
      fill="none"
      stroke-width={thickness}
      stroke-dasharray="{dash},100"
      stroke-linecap="round"
    />
  </svg>
</Modal>

<style>
  .svelte-spinner {
    transition-property: transform;
    animation-name: svelte-spinner_infinite-spin;
    animation-iteration-count: infinite;
    animation-timing-function: linear;
    position: relative;
    z-index: 1000;
    margin-left: var(--margin-left);
  }
  @keyframes svelte-spinner_infinite-spin {
    from {
      transform: rotate(0deg);
    }
    to {
      transform: rotate(360deg);
    }
  }
</style>
