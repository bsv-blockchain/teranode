<svelte:options runes={true} />

<script lang="ts">
  import BlocksTableCard from '$internal/components/page/viewer/blocks/blocks-table-card/index.svelte'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
</script>

<PageWithMenu testId="page-root">
  <BlocksTableCard />
</PageWithMenu>

<style>
</style>
