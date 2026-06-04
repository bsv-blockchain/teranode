<svelte:options runes={true} />

<script lang="ts">
  import { Tab, Dropdown, TextInput } from '$lib/components'
  import Typo from '../typo/index.svelte'
  import { getBtnData, getPageSizeOptions } from './utils'
  import type { I18n } from '$lib/types'

  let type = 'page'

  let {
    testId = null,
    name = '',
    value = $bindable(),
    totalItems = 0,
    siblingCount = 0,
    boundaryCount = 1,
    hasBoundaryRight = true,
    dataSize = 5,
    i18n,
    expandUp = false,
    stretch = true,
    showPageSize = true,
    showQuickNav = true,
    showNav = true,
    onchange,
  }: {
    testId?: string | undefined | null
    name?: string
    value?: { page?: number; pageSize?: number }
    totalItems?: number
    siblingCount?: number
    boundaryCount?: number
    hasBoundaryRight?: boolean
    dataSize?: number
    i18n?: I18n | null | undefined
    expandUp?: boolean
    stretch?: boolean
    showPageSize?: boolean
    showQuickNav?: boolean
    showNav?: boolean
    onchange?: (e: { name: string; type: string; value: any }) => void
  } = $props()

  const i18nLocal = $derived({
    t: i18n?.t,
    baseKey: i18n?.baseKey || 'comp.pager',
  })
  const t = $derived(i18nLocal?.t || function () {})
  const baseKey = $derived(i18nLocal?.baseKey)
  const pageSizeOptions = $derived(getPageSizeOptions(i18nLocal))

  const page = $derived(value?.page || 1)
  const pageSize = $derived(value?.pageSize || 10)

  // pageInput is derived from `page` but also reassigned imperatively by the
  // input-change handler, so it cannot be a plain $derived. Keep it as $state
  // and re-sync it whenever `page` changes.
  let pageInput = $state(`${value?.page || 1}`)
  $effect(() => {
    pageInput = `${page}`
  })

  const totalPages = $derived(Math.max(1, Math.ceil(totalItems / pageSize)))
  const isLastPage = $derived(dataSize < pageSize)
  const btnData: { type: 'page' | 'range'; page?: number; range?: number[] }[] = $derived(
    getBtnData(totalPages, page, isLastPage, hasBoundaryRight, boundaryCount, siblingCount),
  )

  function isSelected(btn) {
    return btn.type === 'page' && btn.page === page
  }

  function callChange(page, pageSize) {
    value = { page, pageSize }
    onchange?.({ name, type, value })
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
    const name = e.name
    const val = parseInt(e.value)

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

  const quickNavDisabled = $derived.by(() => {
    const pageNum = parseInt(pageInput)
    return (
      !pageInput || isNaN(pageNum) || pageNum === page || pageNum < 1 || pageNum > totalPages
    )
  })
  const onCurrentPage = $derived(parseInt(pageInput) === page)

  function onQuickNavKeyDown(e) {
    const keyCode = e.code || e.key
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
        onchange={onInputChange}
      />
    </div>
  {/if}
  {#if showQuickNav}
    <div class="quick-nav">
      <Typo variant="text" size="sm" value={t(`${baseKey}.page`)} wrap={false} overflow={true} />
      <div class="quick-input">
        <TextInput
          type="number"
          min={1}
          max={hasBoundaryRight ? totalPages : page + (isLastPage ? 0 : 1)}
          name="page"
          size="small"
          stretch={true}
          value={pageInput}
          valid={!quickNavDisabled || onCurrentPage}
          onchange={onInputChange}
          onkeydown={onQuickNavKeyDown}
        />
      </div>
      <Typo
        variant="text"
        size="sm"
        value={t(`${baseKey}.of_total`, { total: totalPages })}
        wrap={false}
        overflow={true}
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
        onclick={() => onNav('prev')}
      />
      {#each btnData as btn}
        <Tab
          variant="primary"
          size="small"
          selected={isSelected(btn)}
          onclick={() => onSelect(btn)}
        >
          {btn.type === 'page' ? btn.page : '...'}
        </Tab>
      {/each}
      <Tab
        variant="primary"
        iconAfter="icon-chevron-right-line"
        size="small"
        disabled={page === totalPages}
        onclick={() => onNav('next')}
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
  .quick-input {
    min-width: 80px;
    max-width: 130px;
  }

  .page-size {
    display: flex;
    align-items: center;
    gap: 4px;
  }
</style>
