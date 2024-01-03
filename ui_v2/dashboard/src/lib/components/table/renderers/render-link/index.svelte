<script lang="ts">
  import { getLinkPrefix } from '../../../../utils/url'

  export let href = ''
  export let text = null
  export let className = ''
  export let external = true

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
  <a href={hrefWithPrefix} {...props}>{value}</a>
{:else}
  ''
{/if}
