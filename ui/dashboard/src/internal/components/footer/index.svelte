<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import Typo from '../typo/index.svelte'
  import i18n from '../../i18n'

  $: t = $i18n.t
  const tKey = 'comp.footer'

  $: gutterW =
    $mediaSize <= MediaSize.lg
      ? $mediaSize <= MediaSize.sm
        ? $mediaSize <= MediaSize.xs
          ? 16
          : 20
        : 32
      : 90

  export let testId: string | undefined | null = null

  let year = new Date().getFullYear()
</script>

<div class="tui-footer" data-test-id={testId} style:--padding={`0px ${gutterW}px`}>
  <div class="content">
    <div class="left">
      <Typo
        variant="text"
        size="sm"
        value={t(`${tKey}.copyright`, { year })}
        color="rgba(255, 255, 255, 0.66)"
      />
    </div>
    <div class="right">
      <a href={t(`${tKey}.privacy_url`)}>
        <Typo
          variant="text"
          size="sm"
          value={t(`${tKey}.privacy`)}
          color="rgba(255, 255, 255, 0.66)"
          hoverColor="rgba(255, 255, 255, 0.88)"
        />
      </a>
      <a href={t(`${tKey}.terms_url`)}>
        <Typo
          variant="text"
          size="sm"
          value={t(`${tKey}.terms`)}
          color="rgba(255, 255, 255, 0.66)"
          hoverColor="rgba(255, 255, 255, 0.88)"
        />
      </a>
    </div>
  </div>
</div>

<style>
  .tui-footer {
    box-sizing: var(--box-sizing);

    padding: var(--padding);
    margin-top: -1px;

    width: 100%;
    height: var(--footer-height);
    flex: 0;

    background-color: transparent;
  }

  .content {
    height: 100%;

    display: flex;
    align-items: center;
    justify-content: space-between;

    border-top: 1px solid rgba(255, 255, 255, 0.08);
  }

  .right {
    display: flex;
    align-items: center;
    gap: 15px;
  }
  .right a {
    text-decoration: none;
  }
</style>
