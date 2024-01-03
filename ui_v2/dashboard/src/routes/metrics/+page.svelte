<script lang="ts">
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
</script>

<PageWithMenu>
  <!-- svelte-ignore a11y-missing-attribute -->
  <div class="full">
    <iframe
      src="https://grafana.ubsv.dev/d-solo/cde0cde9-5698-4433-b851-ccae709ee2b2/ubsv-service-overview?orgId=1&refresh=5s&panelId=2"
      width="100%"
      height="100%"
      frameborder="0"
    />
  </div>
</PageWithMenu>

<style>
  :root {
    --navbar-height: 52px; /* Define the custom property */
  }

  .full {
    width: 100%;
    height: calc(100vh - var(--navbar-height));
  }
</style>
