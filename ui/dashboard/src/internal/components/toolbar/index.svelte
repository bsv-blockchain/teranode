<script lang="ts">
  import { goto } from '$app/navigation'
  import TextInput from '$lib/components/textinput/index.svelte'
  import BreadCrumbs from '$internal/components/breadcrumbs/index.svelte'
  import { failure } from '$lib/utils/notifications'
  import { getDetailsUrl } from '$internal/utils/urls'

  import i18n from '$internal/i18n'
  import * as api from '$internal/api'

  $: t = $i18n.t

  export let style = ''

  let searchValue = ''
  let lastSearchCalled = ''

  async function onSearchKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.detail.code || e.detail.key

    if (keyCode === 'Enter') {
      lastSearchCalled = searchValue

      const result: any = await api.searchItem({ q: searchValue })
      if (result.ok) {
        const { type, hash } = result.data
        goto(getDetailsUrl(type, hash))
      } else {
        failure(result.error.message)
      }
      return false
    } else if (keyCode === 'Escape') {
      searchValue = ''
      return false
    }
  }
</script>

<div class="toolbar" {style}>
  <div class="left">
    <BreadCrumbs />
  </div>
  <div class="right">
    <TextInput
      name="one"
      size="medium"
      style="--input-size-md-border-radius:8px"
      autocomplete="off"
      bind:value={searchValue}
      width={330}
      focusWidth={570}
      icon={searchValue === lastSearchCalled || !searchValue
        ? 'icon-search-line'
        : 'icon-search-solid'}
      placeholder={$i18n.t('comp.toolbar.placeholder')}
      on:keydown={onSearchKeyDown}
    />
  </div>
</div>

<div class="warning" {style}>{t('global.warning_2')}</div>

<style>
  .toolbar {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 5px;
    flex-wrap: wrap;
  }

  .toolbar .left {
    display: flex;
    justify-content: flex-start;
  }

  .toolbar .right {
    display: flex;
    justify-content: flex-end;
    gap: 4px;
  }

  .warning {
    width: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 10px;
    background-color: var(--color-warning);
    color: var(--color-white);
    font-size: 14px;
  }
</style>
