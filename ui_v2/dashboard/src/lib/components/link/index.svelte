<script lang="ts">
  import { Icon } from '$lib/components'
  import { ComponentSize, FlexDirection, getStyleSizeFromComponentSize } from '$lib/styles/types'
  import type { FlexDirectionType } from '$lib/styles/types'
  import type { InputSizeType } from '$lib/styles/types/input'
  import { getLinkPrefix } from '$lib/utils/url'

  export let testId: string | undefined | null = null

  let clazz: string | undefined | null = null
  export { clazz as class }

  export let style = ''

  export let href = ''
  export let external = false
  export let bold = false

  export let icon: string | undefined | null = null
  export let iconAfter: string | undefined | null = null

  let hasIcon = false
  let direction: FlexDirectionType = FlexDirection.row

  $: {
    hasIcon = Boolean(icon) || Boolean(iconAfter)

    if (hasIcon) {
      direction = icon ? FlexDirection.row : FlexDirection.rowReverse
    }
  }

  export let size: InputSizeType = ComponentSize.medium
  $: styleSize = getStyleSizeFromComponentSize(size)

  $: linkVarStr = `--link-default`
  $: linkSizeStr = `--link-size-${styleSize}`

  let cssVars: string[] = []
  $: {
    let states = ['enabled', 'hover', 'active', 'bold', 'visited']
    cssVars = [
      ...states.reduce(
        (acc, state) => [...acc, `--${state}-color:var(${linkVarStr}-${state}-color)`],
        [] as string[],
      ),
      `--bold-text-decoration-line:var(${linkVarStr}-bold-text-decoration-line)`,
      `--icon-size:var(${linkSizeStr}-icon-size)`,
      `--font-size:var(${linkSizeStr}-font-size)`,
      `--line-height:var(${linkSizeStr}-line-height)`,
      `--letter-spacing:var(${linkSizeStr}-letter-spacing)`,
      `--font-family:var(--link-font-family)`,
      `--font-weight:var(--link-font-weight)`,
      `--gap:var(--link-gap)`,
    ]
  }

  $: fullHref = getLinkPrefix(href, external) + href
</script>

<a
  data-test-id={testId}
  class={`tui-link${clazz ? ' ' + clazz : ''}`}
  class:bold
  style={`${cssVars.join(';')}${style ? `;${style}` : ''}`}
  style:--direction={direction}
  href={fullHref}
  target={external ? '_blank' : '_self'}
  rel={external ? 'noopener noreferrer' : null}
>
  {#if hasIcon}
    <Icon
      class="icon"
      name={icon || iconAfter}
      style={`--width:var(${linkSizeStr}-icon-size);--height:var(${linkSizeStr}-icon-size)`}
    />
  {/if}
  {#if $$slots.default}
    <div class="label"><slot /></div>
  {/if}
</a>

<style>
  .tui-link {
    display: flex;
    flex-direction: var(--direction);
    align-items: center;
    justify-content: center;
    gap: var(--gap);

    outline: none;
    box-sizing: var(--box-sizing);

    font-family: var(--font-family);
    font-size: var(--font-size);
    font-weight: var(--font-weight);
    line-height: var(--line-height);
    letter-spacing: var(--letter-spacing);

    cursor: pointer;

    color: var(--enabled-color);
  }
  .tui-link.bold {
    text-decoration-line: var(--bold-text-decoration-line);
  }

  .tui-link:hover {
    color: var(--hover-color);
  }

  .tui-link:active {
    color: var(--active-color);
  }

  .tui-link:visited {
    color: var(--visited-color);
  }

  .tui-link .label {
    display: flex;
    align-items: center;
    white-space: nowrap;
  }
</style>
