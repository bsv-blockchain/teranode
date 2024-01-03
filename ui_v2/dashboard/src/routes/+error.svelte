<script lang="ts">
  import { page } from '$app/stores'
  import { goto } from '$app/navigation'

  import Button from '$lib/components/button/index.svelte'
  import Logo from '$lib/components/logo/index.svelte'
  import Typo from '$internal/components/typo/index.svelte'
  import i18n from '$internal/i18n'

  const fieldKey = 'comp.error'

  $: t = $i18n.t

  const maxWidth = 500

  $: is404 = $page.status === 404
  $: isFatal = $page.status >= 500 && $page.status < 600

  let logoName = ''
  let title = ''
  let body = ''
  let btnLabel = ''

  $: {
    if (is404) {
      logoName = 'error-404'
      title = t(`${fieldKey}.404.title`)
      body = t(`${fieldKey}.404.body`, { url: $page.url })
      btnLabel = t(`${fieldKey}.404.home`)
    } else if (isFatal) {
      logoName = 'error-fatal'
      title = t(`${fieldKey}.fatal.title`)
      body = $page?.error?.message || ''
      btnLabel = t(`${fieldKey}.fatal.home`)
    } else {
      logoName = 'error-x'
      title = t(`${fieldKey}.x.title`, { status: $page.status })
      body = $page?.error?.message || ''
      btnLabel = t(`${fieldKey}.x.home`)
    }
  }

  function onHome() {
    goto('/')
  }
</script>

<div class="error" style:--max-width={maxWidth}>
  <div class="logo">
    <Logo name={logoName} width={162} />
  </div>
  <div class="title">
    <Typo variant="title" size="h5" value={title} color="rgba(255, 255, 255, 0.88)" />
  </div>
  <div class="body">
    <Typo variant="text" size="md" value={body} color="rgba(255, 255, 255, 0.66)" />
  </div>
  <div class="btn">
    <Button variant="tertiary" width={140} on:click={onHome}>{btnLabel}</Button>
  </div>
</div>

<style>
  .error {
    display: flex;
    flex-direction: column;
    align-items: center;

    padding: 137px 20px 20px 20px;

    max-width: var(--max-width);
    text-align: center;
  }

  .title {
    padding-top: 32px;
    padding-bottom: 8px;
  }

  .btn {
    padding-top: 32px;
  }
</style>
