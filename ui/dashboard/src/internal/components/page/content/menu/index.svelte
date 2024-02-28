<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'

  $: gutterW =
    $mediaSize <= MediaSize.lg
      ? $mediaSize <= MediaSize.sm
        ? $mediaSize <= MediaSize.xs
          ? 16
          : 20
        : 32
      : 90

  const marginTop = 15
  const marginBottom = 36
</script>

<div
  class="page-content"
  style:--padding="0 {gutterW}px"
  style:--margin-top={marginTop + 'px'}
  style:--margin-bottom={marginBottom + 'px'}
  style:--min-height-local="calc(100% - {marginTop + marginBottom}px - var(--footer-height, 0))"
>
  <slot />
</div>

<style>
  .page-content {
    box-sizing: var(--box-sizing);

    margin: var(--margin-top) 0 var(--margin-bottom) 0;
    display: flex;
    flex-direction: column;
    align-items: center;

    width: 100%;
    padding: var(--padding);
    margin-left: 0;

    transition: padding var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
</style>
