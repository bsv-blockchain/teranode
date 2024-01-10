<script lang="ts">
  import { Typo } from '$lib/components'
  import { i18n } from '../../stores/media'

  $: t = $i18n.t
  const tKey = 'comp.footer'

  export let testId: string | undefined | null = null

  let year = new Date().getFullYear()
</script>

<div class="tui-footer" data-test-id={testId}>
  <span><Typo variant="body" size={4} value={t(`${tKey}.copyright`, { year })} /></span>
</div>

<style>
  .tui-footer {
    display: flex;
    align-items: center;
    justify-content: center;

    width: 100%;
    height: var(--footer-height);
    flex: 0;

    background-color: var(--footer-bg-color);
  }

  span {
    margin-top: 4px;
  }
</style>
