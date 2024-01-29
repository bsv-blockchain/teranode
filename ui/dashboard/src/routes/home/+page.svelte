<script lang="ts">
  import { onMount } from 'svelte'
  import HomeStatsCard from '$internal/components/page/home/home-stats-card/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'

  let Graph
  onMount(() => {
    const timeoutId = setTimeout(async () => {
      Graph = (await import('$internal/components/page/home/home-stats-graph/index.svelte')).default
    }, 10)

    return () => clearTimeout(timeoutId)
  })
</script>

<PageWithMenu>
  <div class="content">
    <HomeStatsCard />
    {#if Graph}
      <Graph />
    {/if}
  </div>
</PageWithMenu>

<style>
  .content {
    width: 100%;

    display: flex;
    flex-direction: column;
    gap: 20px;
  }
</style>
