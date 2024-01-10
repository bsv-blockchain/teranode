<script lang="ts">
  import { tweened } from 'svelte/motion'
  import { cubicOut } from 'svelte/easing'

  const progress = tweened(0, {
    duration: 1000,
    easing: cubicOut,
  })

  export let testId: string | undefined | null = null

  // ratio-based progress
  export let ratio = -1
  export let dangerRatio = -1
  export let warnRatio = -1

  // value-based progress
  export let value = -1
  export let total = -1
  export let dangerValue = -1
  export let warnValue = -1

  export let normalColor = '#6EC492'
  export let warnColor = '#F5A200'
  export let dangerColor = '#FF344C'

  let showValue = false
  let valueRatio = 0
  let color = normalColor
  let validRatioSet = false

  export let size = 'medium'

  let height = 12
  let borderRadius = 20

  $: {
    switch (size) {
      case 'large':
      case 'medium':
        height = 12
        borderRadius = 20
        break
      case 'small':
        height = 3
        borderRadius = 5
        break
    }
  }

  $: {
    showValue = false
    validRatioSet = ratio !== -1 && ratio >= 0 && ratio <= 1

    if (validRatioSet) {
      showValue = true

      if (showValue) {
        color = normalColor
        if (dangerRatio !== -1 && dangerRatio <= 1 && ratio <= dangerRatio) {
          color = dangerColor
        } else if (warnRatio !== -1 && warnRatio <= 1 && ratio <= warnRatio) {
          color = warnColor
        }
      } else {
        color = normalColor
      }
    } else {
      if (value !== -1 && total !== -1 && total !== 0 && value <= total) {
        showValue = true
      }

      if (showValue) {
        valueRatio = value / total
      } else {
        valueRatio = 0
      }

      if (showValue) {
        color = normalColor
        if (dangerValue !== -1 && dangerValue <= total && value <= dangerValue) {
          color = dangerColor
        } else if (warnValue !== -1 && warnValue <= total && value <= warnValue) {
          color = warnColor
        }
      } else {
        color = normalColor
      }
    }

    if (showValue) {
      progress.set(validRatioSet ? ratio : valueRatio)
    } else {
      progress.set(0)
    }
  }
</script>

<div
  class="tui-progress"
  data-test-id={testId}
  style:--value-width-local="{$progress * 100}%"
  style:--value-color-local={color}
  style:--height-local={height + 'px'}
  style:--border-radius-local={borderRadius + 'px'}
  style:--value-border-radius-local="{borderRadius}px 0 0 {borderRadius}px"
>
  {#if showValue}
    <div class="value" />
  {/if}
</div>

<style>
  .tui-progress {
    background-color: #efefef;
    border-radius: var(--border-radius-local);
    height: var(--height-local);
    width: 100%;
    overflow: hidden;
  }

  .value {
    background-color: var(--value-color-local);
    border-radius: var(--value-border-radius-local);
    height: 100%;
    width: var(--value-width-local);
  }
</style>
