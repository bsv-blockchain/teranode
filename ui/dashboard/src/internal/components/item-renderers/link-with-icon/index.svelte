<svelte:options runes={true} />

<script lang="ts">
  import { tippy } from '$lib/stores/media'

  import { Icon } from '$lib/components'
  import { getLinkPrefix } from '$lib/utils/url'

  let {
    href = '',
    text = null,
    className = '',
    external = true,
    iconValue = null,
    icon = '',
    iconSize = 18,
    iconPadding = '2px 0 0 0',
    tooltip = '',
    onIcon = (_value: any) => {},
    onselect,
  }: {
    href?: string
    text?: string | null
    className?: string
    external?: boolean
    iconValue?: any
    icon?: string
    iconSize?: number
    iconPadding?: string
    tooltip?: string
    onIcon?: (value: any) => void
    onselect?: (detail: { value: any }) => void
  } = $props()

  const attrs = $derived.by(() => {
    const a: any = external ? { target: '_blank', rel: 'noopener noreferrer' } : {}
    if (className) {
      a.class = className
    }
    return a
  })

  const hrefWithPrefix = $derived(getLinkPrefix(href, external) + href)

  const value = $derived(text || href)

  function onIconLocal() {
    if (onIcon) {
      onIcon(iconValue)
    }
    onselect?.({ value: iconValue })
  }
</script>

{#if value}
  {#if icon && iconValue}
    <div class="link" style:--icon-padding={iconPadding}>
      <a href={hrefWithPrefix} {...attrs}>{value}</a>
      <div class="icon" use:$tippy={{ content: tooltip }}>
        <Icon name={icon} size={iconSize} onclick={onIconLocal} />
      </div>
    </div>
  {:else}
    <a href={hrefWithPrefix} {...attrs}>{value}</a>
  {/if}
{:else}
  ''
{/if}

<style>
  .link {
    display: flex;
    align-items: flex-start;
    gap: 4px;
  }

  .icon {
    padding: var(--icon-padding);
    cursor: pointer;
  }
</style>
