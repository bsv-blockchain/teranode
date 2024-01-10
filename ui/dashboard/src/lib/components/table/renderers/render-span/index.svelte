<script lang="ts">
  export let value = ''
  export let className = ''

  let props: any = {}

  $: {
    if (className) {
      props.class = className
    }
  }
</script>

{#if value}
  <span {...props}>{value}</span>
{:else}
  ''
{/if}
