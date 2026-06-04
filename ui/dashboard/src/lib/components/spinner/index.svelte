<svelte:options runes={true} />

<script lang="ts">
  import { Modal } from '$lib/components'

  let {
    size = 75,
    speed = 550,
    color = '#232d7c',
    coverColor = 'rgba(255, 255, 255, 0.7)',
    thickness = 1,
    gap = 25,
    radius = 10,
    offsetX = 0,
  }: {
    size?: number | 'small'
    speed?: number
    color?: string
    coverColor?: string
    thickness?: number
    gap?: number
    radius?: number
    offsetX?: number
  } = $props()

  // Handle predefined sizes
  const isSmall = $derived(size === 'small')
  const effectiveSize = $derived(isSmall ? 16 : (size as number))
  const effectiveThickness = $derived(isSmall ? 2 : thickness)
  const effectiveRadius = $derived(isSmall ? 6 : radius)
  const effectiveGap = $derived(isSmall ? 20 : gap)

  const dash = $derived((2 * Math.PI * effectiveRadius * (100 - effectiveGap)) / 100)
  const marginLeft = $derived(offsetX)
</script>

<Modal flyContent={false} coverCol={coverColor}>
  <svg
    height={effectiveSize}
    width={effectiveSize}
    style="animation-duration:{speed}ms;"
    class="svelte-spinner {effectiveSize === 16 ? 'spinner-small' : ''}"
    viewBox="0 0 32 32"
    style:--margin-left={marginLeft + 'px'}
  >
    <circle
      role="presentation"
      cx="16"
      cy="16"
      r={effectiveRadius}
      stroke={color}
      fill="none"
      stroke-width={effectiveThickness}
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
