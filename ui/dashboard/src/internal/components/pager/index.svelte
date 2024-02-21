<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { Tab, Dropdown, TextInput } from '$lib/components'
  import Typo from '../typo/index.svelte'
  import { getBtnData, getPageSizeOptions } from './utils'
  import type { I18n } from '$lib/types'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  let type = 'page'

  export let name = ''
  export let value
  export let totalItems = 0
  export let siblingCount = 0
  export let boundaryCount = 1
  export let hasBoundaryRight = true
  export let dataSize = 5
  export let i18n: I18n | null | undefined
  export let expandUp = false
  export let stretch = true
  export let showPageSize = true
  export let showQuickNav = true
  export let showNav = true

  $: i18nLocal = {
    t: i18n?.t,
    baseKey: i18n?.baseKey || 'comp.pager',
  }
  $: t = i18nLocal?.t || function () {}
  $: baseKey = i18nLocal?.baseKey
  $: pageSizeOptions = getPageSizeOptions(i18nLocal)

  $: page = value?.page || 1
  $: pageSize = value?.pageSize || 10
  $: pageInput = `${page}`

  let totalPages = 1
  let isLastPage = false
  let btnData: { type: 'page' | 'range'; page?: number; range?: number[] }[] = []

  $: {
    totalPages = Math.max(1, Math.ceil(totalItems / pageSize))
    isLastPage = dataSize < pageSize

    btnData = getBtnData(
      totalPages,
      page,
      isLastPage,
      hasBoundaryRight,
      boundaryCount,
      siblingCount,
    )

    dispatch('total', { total: totalPages })
  }

  function isSelected(btn) {
    return btn.type === 'page' && btn.page === page
  }

  function callChange(page, pageSize) {
    value = { page, pageSize }
    dispatch('change', { name, type, value })
  }

  function onSelect(btn) {
    let newPage = 1
    if (btn.type === 'page') {
      newPage = btn.page
    } else {
      newPage = btn.range[0] + Math.floor((btn.range[1] - btn.range[0]) / 2)
    }
    callChange(newPage, pageSize)
  }

  function onNav(action, page_?) {
    switch (action) {
      case 'nav':
        callChange(page_, pageSize)
        break
      case 'prev':
        onNav('nav', Math.max(1, page - 1))
        break
      case 'next':
        onNav('nav', Math.min(totalPages, page + 1))
        break
    }
  }

  const onInputChange = (e) => {
    const name = e.detail.name
    const val = parseInt(e.detail.value)

    switch (name) {
      case 'page':
        if (!isNaN(val)) {
          pageInput = `${val}`
        } else {
          pageInput = ''
        }
        break
      case 'pageSize':
        // let's reset page to 1, as current page index can become out of range
        callChange(1, val)
        break
      default:
    }
  }

  let quickNavDisabled = false
  let onCurrentPage = false

  $: {
    const pageNum = parseInt(pageInput)

    quickNavDisabled =
      !pageInput || isNaN(pageNum) || pageNum === page || pageNum < 1 || pageNum > totalPages

    onCurrentPage = pageNum === page
  }

  function onQuickNavKeyDown(e) {
    const keyCode = e.detail.code || e.detail.key
    if (!quickNavDisabled && keyCode === 'Enter') {
      onNav('nav', parseInt(pageInput))
      return false
    }
  }
</script>

<div class="tui-pager" data-test-id={testId} class:stretch>
  {#if showPageSize}
    <div class="page-size">
      <Typo variant="text" size="sm" value={t(`${baseKey}.show`)} wrap={false} />
      <Dropdown
        name="pageSize"
        value={pageSize}
        items={pageSizeOptions}
        size="small"
        {expandUp}
        on:change={onInputChange}
      />
    </div>
  {/if}
  {#if showQuickNav}
    <div class="quick-nav">
      <Typo variant="text" size="sm" value={t(`${baseKey}.page`)} wrap={false} />
      <TextInput
        type="number"
        min={1}
        max={hasBoundaryRight ? totalPages : page + (isLastPage ? 0 : 1)}
        name="page"
        size="small"
        value={pageInput}
        valid={!quickNavDisabled || onCurrentPage}
        on:change={onInputChange}
        on:keydown={onQuickNavKeyDown}
      />
      <Typo
        variant="text"
        size="sm"
        value={t(`${baseKey}.of_total`, { total: totalPages })}
        wrap={false}
      />
    </div>
  {/if}
  {#if showNav}
    <div class="btns">
      <Tab
        variant="primary"
        icon="icon-chevron-left-line"
        size="small"
        disabled={page === 1}
        on:click={() => onNav('prev')}
      />
      {#each btnData as btn}
        <Tab
          variant="primary"
          size="small"
          selected={isSelected(btn)}
          on:click={() => onSelect(btn)}
        >
          {btn.type === 'page' ? btn.page : '...'}
        </Tab>
      {/each}
      <Tab
        variant="primary"
        iconAfter="icon-chevron-right-line"
        size="small"
        disabled={page === totalPages}
        on:click={() => onNav('next')}
      />
    </div>
  {/if}
</div>

<style>
  .tui-pager {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    float: left;
    display: flex;
    justify-content: space-between;
    flex-wrap: wrap;
    flex: 0;
    gap: 10px;
  }
  .tui-pager.stretch {
    float: none;
    justify-content: space-between;
    width: 100%;
  }

  .btns {
    box-sizing: var(--box-sizing);

    display: flex;
    align-items: center;
    gap: 6px;
  }

  .quick-nav {
    display: flex;
    align-items: center;
    justify-content: stretch;
    gap: 4px;
  }

  .page-size {
    display: flex;
    align-items: center;
    gap: 4px;
  }
</style>
