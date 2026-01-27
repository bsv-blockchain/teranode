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

{#if value || value === 0}
  <span {...props}>{value}</span>
{/if}
