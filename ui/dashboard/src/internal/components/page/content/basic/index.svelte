<script lang="ts">
  import { contentLeft } from '../../../../stores/nav'

  export let gutterW = 60
</script>

<div
  class="page-content"
  style:--width-local="calc(100% - {gutterW * 2 + $contentLeft}px)"
  style:--padding-local="0 {gutterW}px"
  style:--margin-left-local={$contentLeft + 'px'}
>
  <slot />
</div>

<style>
  .page-content {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: space-around;

    min-height: calc(100% - var(--footer-height));

    width: var(--width-local);
    max-width: var(--width-local);
    padding: var(--padding-local);
    margin-left: var(--margin-left-local);

    /* overflow: hidden; */
  }
</style>
