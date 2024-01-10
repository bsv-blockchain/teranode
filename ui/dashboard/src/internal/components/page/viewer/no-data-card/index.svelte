<script lang="ts">
  import Button from '$lib/components/button/index.svelte'
  import Logo from '$lib/components/logo/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import { reverseHashParam } from '$internal/utils/urls'
  import i18n from '$internal/i18n'

  const fieldKey = 'comp.no-data'

  $: t = $i18n.t

  export let hash = ''
  export let maxWidth = 400

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
      color="rgba(255, 255, 255, 0.88)"
    />
  </div>
  <div class="body">
    <Typo
      variant="text"
      size="md"
      value={t(`${fieldKey}.body`)}
      color="rgba(255, 255, 255, 0.66)"
    />
  </div>
  <div class="btn">
    <Button variant="tertiary" icon="icon-reeverse-line" width={100} on:click={onReverseHash}
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
