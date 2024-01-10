<script lang="ts">
  import { tippy } from '$lib/stores/media'

  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  import { getLinkPrefix } from '$lib/utils/url'

  export let href = ''
  export let text: string | null = null
  export let className = ''
  export let external = true

  export let iconValue: any = null
  export let icon = 'icon-duplicate-line'
  export let iconSize = 15
  export let iconPadding = '2px 0 0 0'
  export let tooltip = ''

  let props: any = {}
  let hrefWithPrefix = ''
  let value = ''

  $: {
    props = external ? { target: '_blank', rel: 'noopener noreferrer' } : {}

    if (className) {
      props.class = className
    }

    const prefix = getLinkPrefix(href, external)
    hrefWithPrefix = prefix + href

    value = text || href
  }
</script>

{#if value}
  {#if icon && iconValue}
    <div class="link" style:--icon-padding={iconPadding}>
      <a href={hrefWithPrefix} {...props}>{value}</a>
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
    <a href={hrefWithPrefix} {...props}>{value}</a>
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
