<script lang="ts">
  import Box from '../box/index.svelte'
</script>

<Box row {...$$props}>
  <slot />
</Box>
