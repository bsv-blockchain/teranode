<script lang="ts">
  export let text = ''
</script>

<div class="tui-banner">
  {text}
</div>

<style>
  .tui-banner {
    position: sticky;
    top: 0;
    left: 0;
    right: 0;
    height: var(--banner-height);

    display: flex;
    align-items: center;
    justify-content: center;

    background: var(--banner-bg-color);
    color: var(--banner-color);

    font-size: 12px;
    font-style: normal;
    font-weight: 400;
    line-height: 18px; /* 150% */
    letter-spacing: 0.24px;
  }
</style>
