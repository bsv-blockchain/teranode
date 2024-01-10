<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { tippy } from '$lib/stores/media'

  import { Icon } from '$lib/components'
  import { getLinkPrefix } from '$lib/utils/url'

  const dispatch = createEventDispatcher()

  export let href = ''
  export let text: string | null = null
  export let className = ''
  export let external = true

  export let iconValue: any = null
  export let icon = ''
  export let iconSize = 18
  export let iconPadding = '2px 0 0 0'
  export let tooltip = ''
  export let onIcon = (any) => {}

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

  function onIconLocal() {
    if (onIcon) {
      onIcon(iconValue)
    }
    dispatch('select', { value: iconValue })
  }
</script>

{#if value}
  {#if icon && iconValue}
    <div class="link" style:--icon-padding={iconPadding}>
      <a href={hrefWithPrefix} {...props}>{value}</a>
      <div class="icon" use:$tippy={{ content: tooltip }}>
        <Icon name={icon} size={iconSize} on:click={onIconLocal} />
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
