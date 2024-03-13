<script lang="ts">
  import { createEventDispatcher } from 'svelte'

  import { mediaSize, MediaSize } from '../../../../stores/media'
  import { Button, Checkbox, Icon, Pager } from '$lib/components'
  import { getDisplay, SortOrder } from '../../utils'
  import type { ColDef } from '../../types'

  const dispatch = createEventDispatcher()

  export let name
  export let colDefs: ColDef[] = []
  export let data = []
  export let idField
  export let renderCells = {}
  export let renderTypes = {}
  export let disabled = false
  export let expandUp = false
  export let i18n
  export let maxHeight = -1
  export let fullWidth
  export let bgColorTable
  export let bgColorHead
  export let selectable
  export let selectedRowIds = []
  export let filtersEnabled
  export let filtersState
  export let sortEnabled
  export let sortState
  export let paginationEnabled
  export let paginationState
  export let totalItems = -1
  export let hasBoundaryRight = true
  export let pager = true
  export let alignPager = 'center'
  export let getRowIconActions
  export let getRenderProps
  // export let getRowClassName
  export let wrapTitles = true

  function onFilterClick(colId) {
    dispatch('filter', { colId })
  }

  function onHeaderClick(colId) {
    dispatch('header', { colId })
  }

  function onRowSelect(id) {
    dispatch('select', { id })
  }

  function onPage(e) {
    dispatch('paginate', { ...e.detail })
  }

  function onActionIcon(type, value) {
    dispatch('action', { name, type, value })
  }

  let gutter = 32
  let iconsW = 0

  $: {
    iconsW = 0
    let maxIconsW = 0
    data.forEach((item) => {
      const icons = getRowIconActions ? getRowIconActions(name, item, idField) || [] : []
      const w = icons.length > 0 ? icons.length * 40 - 4 : 0
      if (w > maxIconsW) {
        maxIconsW = w
      }
    })
    // table cell has padding-left of 15px
    iconsW = maxIconsW === 0 ? 0 : $mediaSize <= MediaSize.sm ? 56 : maxIconsW + 48
  }

  let gridGap = 0
  $: fixedWidth =
    iconsW + (selectable ? 32 : 0) + (colDefs.length > 0 ? (colDefs.length - 1) * gridGap : 0)
  let grid = ''

  $: {
    let gridItems = selectable ? ['32px'] : []
    if ($mediaSize <= MediaSize.sm) {
      gridItems.push(`calc(100% - ${selectable ? '80px' : iconsW > 0 ? '48px' : '0'})`)
      if (iconsW > 0) {
        gridItems.push(`48px`)
      }
    } else {
      colDefs.forEach((colDef) => {
        gridItems.push(
          colDef?.props?.width
            ? `calc(${colDef.props.width} - (${fixedWidth}px / ${colDefs.length}))`
            : `calc((100% - ${fixedWidth}px) / ${colDefs.length})`,
        )
      })
      if (iconsW > 0) {
        gridItems.push(`${iconsW}px`)
      }
    }
    grid = gridItems.join(' ')
  }

  function getStyleProps(colDef) {
    if (colDef?.props?.style) {
      let str = ''
      Object.keys(colDef?.props?.style).forEach((key) => {
        str += `${key}: ${colDef.props.style[key]};`
      })
      return { style: str }
    }
    return {}
  }

  let cssVars: string[] = []
  $: {
    cssVars = [
      `--max-height-local:${maxHeight + 'px'}`,
      `--bg-col-head-local:${
        bgColorHead
          ? bgColorHead
          : bgColorTable
            ? bgColorTable
            : 'var(--table-th-bg-color, #ffffff)'
      }`,
      `--row-col-local:${bgColorTable ? bgColorTable : 'var(--table-bg-color, none)'}`,
      `--align-pager-local:${alignPager}`,
      `--grid-local:${grid}`,
      `--grid-gap-local:${gridGap + 'px'}`,
      `--header-wrap-titles:${wrapTitles ? 'wrap' : 'nowrap'}`,
    ]
  }
</script>

<div class="table-and-tools" style={`${cssVars.join(';')}`}>
  <div
    class="table-container"
    class:fullWidth
    class:maxHeight={maxHeight !== -1 && !isNaN(maxHeight)}
  >
    <section
      class="table"
      class:small={$mediaSize <= MediaSize.sm}
      class:xsmall={$mediaSize <= MediaSize.xs}
      class:selectable
      class:sortEnabled
      class:hasPager={paginationEnabled && pager}
    >
      {#if $mediaSize > MediaSize.sm}
        <section class="thead">
          <div class="tr">
            {#if selectable}
              <div class="th" />
            {/if}
            {#each colDefs as colDef, i (colDef.id)}
              <!-- svelte-ignore a11y-click-events-have-key-events -->
              <div
                class="th"
                on:click={() => onHeaderClick(colDef.id)}
                class:right={i > 0 && colDef?.type === 'number'}
              >
                <div class="table-cell-row">
                  {colDef.name}
                  {#if sortEnabled && sortState.sortColumn === colDef.id}
                    <div class="header-icon">
                      <Icon
                        name={sortState.sortOrder === SortOrder.asc ? 'chevron-up' : 'chevron-down'}
                        size={18}
                      />
                    </div>
                  {/if}
                  {#if filtersEnabled && filtersState[colDef.id]}
                    <div class="header-icon">
                      <Icon name="filters" size={18} on:click={() => onFilterClick(colDef.id)} />
                    </div>
                  {/if}
                </div>
              </div>
            {/each}
            {#if getRowIconActions}
              <div class="th" />
            {/if}
          </div>
        </section>
      {/if}
      <section class="tbody">
        {#each data as item (item[idField])}
          <div class="tr">
            {#if selectable}
              <div class="td">
                <Checkbox
                  name={item[idField]}
                  checked={selectedRowIds.includes(item[idField])}
                  on:change={() => onRowSelect(item[idField])}
                />
              </div>
            {/if}
            {#if $mediaSize > MediaSize.sm}
              {#each colDefs as colDef, i (colDef.id)}
                <div class="td" class:left={i === 0}>
                  {#if getDisplay(renderCells, renderTypes, colDef, idField, item).component}
                    <svelte:component
                      this={getDisplay(renderCells, renderTypes, colDef, idField, item).component}
                      {...{
                        ...getDisplay(renderCells, renderTypes, colDef, idField, item).props,
                        ...(getRenderProps ? getRenderProps(name, colDef, idField, item) : {}),
                      }}
                    />
                  {:else}
                    {getDisplay(renderCells, renderTypes, colDef, idField, item).value}
                  {/if}
                </div>
              {/each}
            {:else}
              <div class="td">
                <div class="inner-grid">
                  {#each colDefs as colDef (colDef.id)}
                    <div class="inner-grid-item" {...getStyleProps(colDef)}>
                      <!-- svelte-ignore a11y-click-events-have-key-events -->
                      <div class="inner-grid-item-label" on:click={() => onHeaderClick(colDef.id)}>
                        <div class="table-cell-row">
                          {colDef.name}
                          {#if sortEnabled && sortState.sortColumn === colDef.id}
                            <div class="header-icon">
                              <Icon
                                name={sortState.sortOrder === SortOrder.asc
                                  ? 'chevron-up'
                                  : 'chevron-down'}
                                size={18}
                              />
                            </div>
                          {/if}
                          {#if filtersEnabled && filtersState[colDef.id]}
                            <div class="header-icon">
                              <Icon
                                name="filters"
                                size={18}
                                on:click={() => onFilterClick(colDef.id)}
                              />
                            </div>
                          {/if}
                        </div>
                      </div>
                      <div class="inner-grid-item-value">
                        {#if getDisplay(renderCells, renderTypes, colDef, idField, item).component}
                          <svelte:component
                            this={getDisplay(renderCells, renderTypes, colDef, idField, item)
                              .component}
                            {...{
                              ...getDisplay(renderCells, renderTypes, colDef, idField, item).props,
                              ...(getRenderProps
                                ? getRenderProps(name, colDef, idField, item)
                                : {}),
                            }}
                          />
                        {:else}
                          {getDisplay(renderCells, renderTypes, colDef, idField, item).value}
                        {/if}
                      </div>
                    </div>
                  {/each}
                </div>
              </div>
            {/if}
            {#if getRowIconActions}
              <div class="td" class:column={$mediaSize <= MediaSize.sm}>
                {#if !disabled}
                  <div class="table-cell-row">
                    {#each getRowIconActions(name, item, idField) || [] as actionItem (actionItem.icon)}
                      <!-- svelte-ignore a11y-click-events-have-key-events -->
                      <div
                        class="action"
                        class:disabled={actionItem.disabled}
                        on:click={actionItem.disabled
                          ? null
                          : () => onActionIcon(actionItem.type, item)}
                      >
                        {#if actionItem.render === 'icon'}
                          <div class="ico">
                            <Icon name={actionItem.icon} size={18} />
                          </div>
                        {:else}
                          <div class="btn">
                            <Button
                              variant="ghost"
                              size="small"
                              hasFocusRect={false}
                              disabled={actionItem.disabled}
                              icon={actionItem.icon}
                            />
                          </div>
                        {/if}
                      </div>
                    {/each}
                  </div>
                {/if}
              </div>
            {/if}
          </div>
        {/each}
      </section>
    </section>
  </div>
  {#if paginationEnabled && pager}
    <div class="table-pager">
      <Pager
        {i18n}
        {expandUp}
        {totalItems}
        value={paginationState}
        {hasBoundaryRight}
        on:change={onPage}
      />
    </div>
  {/if}
</div>

<style>
  .table-and-tools {
    width: 100%;
    display: flex;
    flex-direction: column;
    align-items: stretch;

    font-family: var(--font-family);
    box-sizing: var(--box-sizing);
  }

  .table-container {
    /* overflow-x: auto; */
    overflow-x: hidden;
  }
  .table-container.fullWidth {
    width: 100%;
  }
  .table-container.maxHeight {
    overflow-y: auto;
    max-height: var(--max-height-local);
  }

  .table {
    font-size: 14px;
    white-space: var(--header-wrap-titles);

    background-color: var(--table-bg-color);
    border-top-left-radius: var(
      --table-border-top-left-radius,
      var(--table-th-border-top-left-radius, 0)
    );
    border-top-right-radius: var(
      --table-border-top-right-radius,
      var(--table-th-border-top-right-radius, 0)
    );
    border-bottom-left-radius: var(--table-border-bottom-left-radius, 0);
    border-bottom-right-radius: var(--table-border-bottom-right-radius, 0);
  }
  .table-container.fullWidth .table {
    width: 100%;
  }

  .table.maxHeight .thead .th {
    position: sticky;
    top: 0;
    box-shadow: 0 2px 1px -1px rgb(0 0 0 / 5%);
  }

  .table .tr {
    margin: 0;
    height: 60px;
    display: grid;
    grid-template-columns: var(--grid-local);
    gap: var(--grid-gap-local);
  }
  .table .tr:first-child .th:first-child,
  .table.small .tr:first-child .td:first-child {
    border-top-left-radius: var(
      --table-border-top-left-radius,
      var(--table-th-border-top-left-radius, 0)
    );
  }
  .table .thead .tr:first-child .th:first-child {
    border-top-left-radius: var(--table-th-border-top-left-radius, 0);
    border-bottom-left-radius: var(--table-th-border-bottom-left-radius, 0);
  }
  .table .tr:first-child .th:last-child,
  .table.small .tr:first-child .td:last-child {
    border-top-right-radius: var(
      --table-border-top-right-radius,
      var(--table-th-border-top-right-radius, 0)
    );
  }
  .table .thead .tr:first-child .th:last-child {
    border-top-right-radius: var(--table-th-border-top-right-radius, 0);
    border-bottom-right-radius: var(--table-th-border-bottom-right-radius, 0);
  }
  .table .tr:last-child .td:first-child {
    border-bottom-left-radius: var(--table-border-bottom-left-radius, 0);
  }
  .table .tr:last-child .td:last-child {
    border-bottom-right-radius: var(--table-border-bottom-right-radius, 0);
  }
  .table.hasPager .tr:last-child .td:first-child {
    border-bottom-left-radius: 0;
  }
  .table.hasPager .tr:last-child .td:last-child {
    border-bottom-right-radius: 0;
  }

  .table .thead .tr {
    height: var(--table-th-height);
  }

  .table .th {
    text-align: left;
    white-space: var(--header-wrap-titles);

    font-weight: var(--table-th-font-weight, 600);
    font-size: var(--table-th-font-size, 13px);
    line-height: var(--table-th-line-height, 16px);
    letter-spacing: var(--table-th-letter-spacing, 0.02em);

    overflow: hidden;
    display: flex;
    align-items: center;

    padding: var(--table-th-padding, 0px 32px);
    text-transform: var(--table-th-text-transform, uppercase);
    color: var(--table-th-color, #232d7c);
    background-color: var(--bg-col-head-local);

    cursor: auto;
  }
  .table.selectable .th {
    padding: 5px;
  }
  .table.sortEnabled .th {
    cursor: pointer;
  }

  .table .th + .th {
    padding-left: 15px;
  }
  .table.selectable .th + .th {
    padding-left: 10px;
  }

  .table .td {
    font-weight: var(--table-td-font-weight, 400);
    font-size: var(--table-td-font-size, 14px);
    line-height: var(--table-td-line-height, 24px);
    letter-spacing: var(--table-td-letter-spacing, -0.01em);

    overflow: hidden;
    display: flex;
    align-items: center;

    color: var(--table-td-color, #282933);

    padding: var(--table-td-padding, 0px 32px);
    border-top: var(--table-td-border-top, 1px solid #efefef);
    border-bottom: var(--table-td-border-bottom, 1px solid #fcfcff);
    background-color: var(--row-col-local);
  }
  .table .tr:first-child .td {
    border-top: var(
      --table-tr-first-child-td-border-top,
      var(--table-td-border-top, 1px solid #efefef)
    );
  }
  .table .tr:last-child .td {
    border-bottom: var(
      --table-tr-last-child-td-border-bottom,
      var(--table-td-border-bottom, 1px solid #fcfcff)
    );
  }
  .table.hasPager .tr:last-child {
    border-bottom: var(--table-td-border-top, 1px solid #efefef) !important;
  }
  .table.selectable .td {
    padding: 5px;
  }

  .table .td + .td {
    padding-left: 15px;
  }
  .table.selectable .td + .td {
    padding-left: 10px;
  }

  .table.small .tbody .td {
    border-top: 1px solid transparent;
    border-bottom: 1px solid transparent;
    padding: 0;
  }
  .table.small .tbody .tr {
    border-top: var(--table-td-border-top, 1px solid #efefef);
    border-bottom: var(--table-td-border-bottom, 1px solid #fcfcff);
    background-color: var(--row-col-local);
    height: inherit;
  }
  .table.small .tbody .tr:first-of-type {
    border-top: 1px solid transparent;
  }
  .table.small .tbody .tr:first-of-type {
    border-top: var(--table-tr-first-child-td-border-top, 1px solid transparent);
  }
  .table.small .tbody .tr:last-of-type {
    border-bottom: var(--table-tr-last-child-td-border-bottom, 1px solid transparent);
  }
  .inner-grid {
    display: grid;
    grid-template-columns: 50% 50%;
    row-gap: 8px;
    column-gap: 0;
    /* display: flex;
    flex-wrap: wrap;
    gap: 8px; */
    width: 100%;
    height: calc(100% - 32px);
    padding: 16px 24px;
  }
  .table.xsmall .inner-grid {
    grid-template-columns: 100%;
  }
  .inner-grid-item {
    display: flex;
    flex-direction: column;
    min-width: calc(50% - 4px);

    padding-right: 15px;
  }
  .inner-grid-item-label {
    text-align: left;
    white-space: nowrap;

    font-size: var(--table-th-font-size, 13px);
    line-height: var(--table-th-line-height, 16px);
    letter-spacing: var(--table-th-letter-spacing, 0.02em);

    overflow: hidden;
    display: flex;
    align-items: center;

    text-transform: var(--table-th-text-transform, uppercase);
    color: var(--table-th-small-color);
    font-weight: var(--table-th-small-font-weight);

    cursor: auto;
  }
  .table.sortEnabled .inner-grid-item-label {
    cursor: pointer;
  }

  .table-cell-row {
    display: flex;
    align-items: center;
    flex-wrap: nowrap;
    gap: 4px;
  }
  .td.column {
    padding: 8px 16px 8px 0 !important;
  }
  .td.column .table-cell-row {
    flex-direction: column;
    justify-content: flex-start;
    height: 100%;
  }

  .header-icon {
    width: 18px;
    height: 18px;
    padding: 0 1px 0 3px;
  }
  .action {
    cursor: pointer;
    color: #232d7c;
  }
  .action .ico {
    width: 18px;
    height: 18px;
    margin: 9px;
  }
  .action .btn {
    width: 36px;
    height: 36px;

    display: flex;
    align-items: center;
    justify-content: center;
  }
  .action.disabled {
    cursor: auto;
  }
  .table-pager {
    flex: 1;
    padding: 10px;
    display: flex;
    align-items: center;
    justify-content: var(--align-pager-local);

    background: #ffffff;
    border-radius: 0px 0px 4px 4px;
  }
  :global(.table .th.right .table-cell-row) {
    text-align: right;
    justify-content: flex-end;
    width: 100%;
  }
  :global(.table .td .num) {
    text-align: right;
    display: block;
    width: 100%;
  }
  :global(.table.small .num),
  :global(.table .td.left .num) {
    text-align: left;
    display: inline;
    width: auto;
  }
</style>
