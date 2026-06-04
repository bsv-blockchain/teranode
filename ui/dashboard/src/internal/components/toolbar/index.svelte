<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation'
  import { mediaSize, MediaSize, theme } from '$lib/stores/media'
  import TextInput from '$lib/components/textinput/index.svelte'
  import Icon from '$lib/components/icon/index.svelte'
  import BreadCrumbs from '$internal/components/breadcrumbs/index.svelte'
  import { failure } from '$lib/utils/notifications'
  import { getDetailsUrl } from '$internal/utils/urls'

  import i18n from '$internal/i18n'
  import * as api from '$internal/api'

  let {
    style = '',
    showTools = true,
  }: {
    style?: string
    showTools?: boolean
  } = $props()

  let searchValue = $state('')
  let lastSearchCalled = $state('')

  async function onSearchKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key

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

  let w = $state(0)

  const focusWidth = $derived($mediaSize <= MediaSize.xs ? w - 60 : 570)

  function toggleTheme() {
    $theme = $theme === 'dark' ? 'light' : 'dark'
  }
</script>

<svelte:window bind:innerWidth={w} />

{#if showTools}
  <div class="toolbar" {style}>
    <div class="left">
      <BreadCrumbs />
    </div>
    <div class="right">
      <button
        class="theme-toggle"
        onclick={toggleTheme}
        type="button"
        title={$theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
        aria-label={$theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}
        aria-pressed={$theme === 'light'}
      >
        <Icon name={$theme === 'dark' ? 'icon-sun-line' : 'icon-moon-line'} size={18} />
      </button>
      <TextInput
        name="one"
        size="medium"
        style="--input-size-md-border-radius:8px"
        autocomplete="off"
        bind:value={searchValue}
        width={330}
        {focusWidth}
        icon={searchValue === lastSearchCalled || !searchValue
          ? 'icon-search-line'
          : 'icon-search-solid'}
        placeholder={$i18n.t('comp.toolbar.placeholder')}
        onkeydown={onSearchKeyDown}
      />
    </div>
  </div>
{/if}


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
    flex: 1;
  }

  .toolbar .right {
    display: flex;
    justify-content: flex-end;
    gap: 4px;
    align-items: center;
  }

  .theme-toggle {
    display: flex;
    align-items: center;
    justify-content: center;
    background: none;
    border: none;
    padding: 6px;
    cursor: pointer;
    color: var(--comp-label-color);
    border-radius: 6px;
    opacity: 0.7;
    transition: opacity 0.15s ease;
  }

  .theme-toggle:hover {
    opacity: 1;
  }
</style>
