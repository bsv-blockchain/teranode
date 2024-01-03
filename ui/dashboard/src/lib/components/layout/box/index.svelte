<script lang="ts">
  export let row = true
  export let wrap = true
  export let reverse = false
  export let stretchWidth = false
  export let flex = false
  export let center = true
  export let gap = -1
  export let gapH = 10
  export let gapV = 10

  let direction = 'row'

  $: {
    if (row) {
      direction = reverse ? 'row-reverse' : 'row'
    } else {
      direction = reverse ? 'column-reverse' : 'column'
    }

    if (gap !== -1) {
      gapH = gap
      gapV = gap
    }
  }
</script>

<div
  class="tui-box"
  style:--direction-local={direction}
  style:--wrap-local={wrap ? 'wrap' : 'nowrap'}
  style:--margin-local="-{gapV}px 0 0 -{gapH}px"
  style:--width-local={stretchWidth ? `calc(100% - ${gapH}px)` : 'auto'}
  style:--justify-content-local={flex ? 'stretch' : center ? 'center' : 'inherit'}
  style:--margin-child-local="{gapV}px 0 0 {gapH}px"
  style:--flex-child-local={flex ? '1' : 'inherit'}
>
  <slot />
</div>

<style>
  .tui-box {
    display: inline-flex;
    flex-direction: var(--direction-local);
    flex-wrap: var(--wrap-local);
    justify-content: var(--justify-content-local);

    margin: var(--margin-local);
    width: var(--width-local);
  }

  .tui-box > :global(*) {
    margin: var(--margin-child-local);
    flex: var(--flex-child-local);
  }
</style>
