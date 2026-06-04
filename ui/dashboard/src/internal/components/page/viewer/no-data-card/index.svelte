<svelte:options runes={true} />

<script lang="ts">
  import Button from '$lib/components/button/index.svelte'
  import Logo from '$lib/components/logo/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import { reverseHashParam } from '$internal/utils/urls'
  import i18n from '$internal/i18n'

  const fieldKey = 'comp.no-data'

  const t = $derived($i18n.t)

  let {
    hash = '',
    maxWidth = 400,
  }: {
    hash?: string
    maxWidth?: number
  } = $props()

  function onReverseHash() {
    reverseHashParam(hash)
  }
</script>

<div class="no-data" style:--max-width={maxWidth}>
  <div class="logo">
    <Logo name="no-data" width={130} />
  </div>
  <div class="title">
    <Typo
      variant="title"
      size="h5"
      value={t(`${fieldKey}.title`)}
      color="var(--app-color)"
    />
  </div>
  <div class="body">
    <Typo
      variant="text"
      size="md"
      value={t(`${fieldKey}.body`)}
      color="var(--comp-label-color)"
    />
  </div>
  <div class="btn">
    <Button variant="tertiary" icon="icon-reeverse-line" width={100} onclick={onReverseHash}
      >{t(`${fieldKey}.reverse`)}</Button
    >
  </div>
</div>

<style>
  .no-data {
    display: flex;
    flex-direction: column;
    align-items: center;

    max-width: var(--max-width);
  }

  .title {
    padding-top: 32px;
    padding-bottom: 8px;
  }

  .btn {
    padding-top: 32px;
  }
</style>
