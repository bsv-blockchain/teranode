<script lang="ts">
  //   import { createEventDispatcher } from 'svelte'

  //   const dispatch = createEventDispatcher()

  //   function toggleMenu() {
  //     dispatch('toggle-menu', {})
  //   }
</script>

<div class="tui-mobile-navbar">
  <!-- <div on:click={(e) => toggleMenu()}>X</div> -->
  <slot />
</div>

<style>
  .tui-mobile-navbar {
    display: flex;
    position: fixed;
    top: 0;
    left: 0;
    right: 0;
    z-index: 3;

    align-items: center;
    justify-content: space-between;
    height: var(--header-height);
    background: var(--comp-bg-color);
    color: var(--comp-color);

    box-shadow: 0px 1px 10px rgba(46, 46, 46, 0.1);

    /* padding: 10px 20px;



    font-family: 'Work Sans', sans-serif;
    font-weight: 500;
    font-size: 9px;
    line-height: 11px;
    letter-spacing: -0.01em;
    text-transform: capitalize;

    color: #232d7c;
    text-align: center; */
  }

  /* .tui-mobile-navbar * {
    cursor: pointer;
  } */
</style>
