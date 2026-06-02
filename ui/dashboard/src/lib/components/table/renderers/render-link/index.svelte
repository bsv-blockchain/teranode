<svelte:options runes={true} />

<script lang="ts">
  import { getLinkPrefix } from '../../../../utils/url'

  let {
    href = '',
    text = null,
    className = '',
    external = true,
  }: { href?: string; text?: any; className?: string; external?: boolean } = $props()

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
  <a href={hrefWithPrefix} {...linkProps}>{value}</a>
{:else}
  ''
{/if}
