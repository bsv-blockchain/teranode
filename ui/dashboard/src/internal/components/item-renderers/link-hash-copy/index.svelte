<svelte:options runes={true} />

<script lang="ts">
  import { tippy } from '$lib/stores/media'

  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  import { getLinkPrefix } from '$lib/utils/url'

  let {
    href = '',
    text = null,
    className = '',
    external = true,
    iconValue = null,
    icon = 'icon-duplicate-line',
    iconSize = 15,
    iconPadding = '2px 0 0 0',
    tooltip = '',
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
  } = $props()

  const linkProps = $derived.by(() => {
    const p: any = external ? { target: '_blank', rel: 'noopener noreferrer' } : {}

    if (className) {
      p.class = className
    }

    return p
  })

  const hrefWithPrefix = $derived(getLinkPrefix(href, external) + href)

  const value = $derived(text || href)
</script>

{#if value}
  {#if icon && iconValue}
    <div class="link" style:--icon-padding={iconPadding}>
      <a href={hrefWithPrefix} {...linkProps}>{value}</a>
      <div class="icon" use:$tippy={{ content: tooltip }}>
        <ActionStatusIcon
          {icon}
          action={copyTextToClipboardVanilla}
          actionData={iconValue}
          size={iconSize}
        />
      </div>
    </div>
  {:else}
    <a href={hrefWithPrefix} {...linkProps}>{value}</a>
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
