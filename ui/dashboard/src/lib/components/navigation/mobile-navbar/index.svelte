<script lang="ts">
  export let offsetTop = ''
</script>

<div class="tui-mobile-navbar" style:--offset-top={offsetTop}>
  <slot />
</div>

<style>
  .tui-mobile-navbar {
    position: fixed;
    top: var(--offset-top);
    left: 0;
    right: 0;
    z-index: 3;

    display: flex;
    align-items: center;
    justify-content: space-between;
    height: var(--header-height);
    background: var(--comp-bg-color);
    color: var(--comp-color);

    box-shadow: 0px 1px 10px rgba(46, 46, 46, 0.1);
  }
</style>
