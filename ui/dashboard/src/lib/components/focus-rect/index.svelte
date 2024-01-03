<script lang="ts">
  export let style = ''

  export let disabled = false
  export let show = true
  export let borderRadius = ''
  export let stretch = true

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--focus-rect-color:${disabled ? 'transparent' : `var(--comp-focus-rect-color)`}`,
      `--focus-rect-width:var(--comp-focus-rect-width)`,
      `--focus-rect-border-radius:${
        borderRadius ? borderRadius : 'var(--comp-focus-rect-border-radius)'
      }`,
      `--focus-rect-padding:var(--comp-focus-rect-padding)`,
      `--focus-rect-bg-color:var(--comp-focus-rect-bg-color)`,
    ]
  }
</script>

{#if show}
  <div
    class="tui-focus-rect"
    class:stretch
    style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  >
    <div class="halo"><slot /></div>
  </div>
{:else}
  <slot />
{/if}

<style>
  .tui-focus-rect {
    box-sizing: var(--box-sizing);

    display: flex;
  }
  .tui-focus-rect.stretch {
    display: block;
  }
  .tui-focus-rect .halo {
    margin: calc(-1 * (var(--focus-rect-width) + var(--focus-rect-padding)));

    padding: var(--focus-rect-padding);

    border-style: solid;
    border-color: transparent;
    border-width: var(--focus-rect-width);
    border-radius: var(--focus-rect-border-radius);

    outline: none;
  }

  /* See: https://larsmagnus.co/blog/focus-visible-within-the-missing-pseudo-class */
  .tui-focus-rect .halo:has(:focus-visible) {
    border-color: var(--focus-rect-color);
    background-color: var(--focus-rect-bg-color);
  }
</style>
