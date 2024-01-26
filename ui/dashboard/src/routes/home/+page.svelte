<script lang="ts">
  import HomeStatsCard from '$internal/components/page/home/home-stats-card/index.svelte'
  import HomeStatsGraph from '$internal/components/page/home/home-stats-graph/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
</script>

<PageWithMenu>
  <div class="content">
    <HomeStatsCard />
    <HomeStatsGraph />
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
