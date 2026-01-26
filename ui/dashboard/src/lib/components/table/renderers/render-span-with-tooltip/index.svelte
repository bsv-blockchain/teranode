<script lang="ts">
  import { tippy } from '$lib/stores/media'
  
  export let value = ''
  export let className = ''
  export let tooltip = ''
  
  let props: any = {}
  
  $: {
    if (className) {
      props.class = className
    }
  }
</script>

{#if value || value === 0}
  {#if tooltip && $tippy}
    <span {...props} use:$tippy={{ content: tooltip }}>{value}</span>
  {:else}
    <span {...props}>{value}</span>
  {/if}
{/if}
