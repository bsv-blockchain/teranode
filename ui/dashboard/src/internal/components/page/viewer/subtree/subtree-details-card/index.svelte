<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { tippy } from '$lib/stores/media'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'

  import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'
  import { DetailType, DetailTab, reverseHashParam, getHashLinkProps } from '$internal/utils/urls'

  import { addNumCommas, dataSize } from '$lib/utils/format'
  import { Button, Icon } from '$lib/components'
  import JSONTree from '$internal/components/json-tree/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import Pager from '$internal/components/pager/index.svelte'
  import i18n from '$internal/i18n'
  import { getItemApiUrl, ItemType } from '$internal/api'
  import * as api from '$internal/api'
  import { failure } from '$lib/utils/notifications'

  const dispatch = createEventDispatcher()

  const baseKey = 'page.viewer-subtree.details'
  const fieldKey = `${baseKey}.fields`

  $: t = $i18n.t
  $: i18nLocal = { t, baseKey: 'comp.pager' }

  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = {}
  export let display: DetailTab = DetailTab.overview
  export let blockHash = ''

  $: expandedData = data?.expandedData
  $: isOverview = display === DetailTab.overview
  $: isJson = display === DetailTab.json
  $: isMerkleProof = display === DetailTab.merkleproof

  let paginatedData: any = null
  let page = 1
  let pageSize = 20
  let totalItems = 0

  $: totalPages = Math.max(1, Math.ceil(totalItems / pageSize))
  $: showPagerNav = totalPages > 1
  $: showPagerSize = showPagerNav || (totalPages === 1 && paginatedData?.Nodes?.length > 5)

  function onDisplay(value) {
    dispatch('display', { value })
  }

  function onReverseHash(hash) {
    reverseHashParam(hash)
  }

  function onPage(e) {
    const pageData = e.detail
    page = pageData.value.page
    pageSize = pageData.value.pageSize
  }

  async function fetchPaginatedData(hash, page, pageSize) {
    const result: any = await api.getSubtreeNodes({
      hash,
      offset: (page - 1) * pageSize,
      limit: pageSize,
    })
    if (result.ok) {
      paginatedData = result.data.data
      const pagination = result.data.pagination
      pageSize = pagination.limit
      page = Math.floor(pagination.offset / pageSize) + 1
      totalItems = pagination.totalRecords
    } else {
      failure(result.error.message)
    }
  }

  $: if (isJson && expandedData?.hash) {
    fetchPaginatedData(expandedData.hash, page, pageSize)
  }
</script>

<Card title={t(`${baseKey}.title`, { height: expandedData.height })}>
  <div class="copy-link" slot="subtitle">
    <div class="hash">{expandedData.hash}</div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-duplicate-line"
        action={copyTextToClipboardVanilla}
        actionData={expandedData.hash}
        size={15}
      />
    </div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-url-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-bracket-line"
        action={copyTextToClipboardVanilla}
        actionData={getItemApiUrl(ItemType.subtree, expandedData.hash)}
        size={15}
      />
    </div>
    <button
      class="icon"
      on:click={() => onReverseHash(expandedData.hash)}
      use:$tippy={{ content: t('tooltip.reverse-hash') }}
      type="button"
    >
      <Icon name="icon-reeverse-line" size={15} />
    </button>
  </div>
  <div class="content">
    <div class="tabs">
      <Button
        size="medium"
        hasFocusRect={false}
        selected={isOverview}
        variant={isOverview ? 'tertiary' : 'primary'}
        on:click={() => onDisplay('overview')}>{t(`${baseKey}.tab.overview`)}</Button
      >
      <Button
        size="medium"
        hasFocusRect={false}
        selected={isJson}
        variant={isJson ? 'tertiary' : 'primary'}
        on:click={() => onDisplay('json')}>{t(`${baseKey}.tab.json`)}</Button
      >
      <Button
        size="medium"
        hasFocusRect={false}
        selected={isMerkleProof}
        variant={isMerkleProof ? 'tertiary' : 'primary'}
        on:click={() => onDisplay('merkleproof')}>{t(`${baseKey}.tab.merkleproof`)}</Button
      >
    </div>
    {#if isOverview}
      <div class="fields" class:collapse>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.block`)}</div>
            <div class="value">
              {#if blockHash}
                <LinkHashCopy {...getHashLinkProps(DetailType.block, blockHash, t)} />
              {:else}
                {t('data.not_available')}
              {/if}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.txCount`)}</div>
            <div class="value">{addNumCommas(expandedData.transactionCount)}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.totalFee`)}</div>
            <div class="value">{addNumCommas(expandedData.fee)}</div>
          </div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.nonce`)}</div>
            <div class="value">TBD</div>
          </div> -->
        </div>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.avgFee`)}</div>
            <div class="value">
              {addNumCommas(expandedData.fee / expandedData.transactionCount)}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.sizeInBytes`)}</div>
            <div class="value">
              {dataSize(expandedData.size)}
            </div>
          </div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.bits`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.confirmations`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.merkleroot`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.chainwork`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.miner`)}</div>
            <div class="value">TBD</div>
          </div> -->
        </div>
      </div>
    {:else if isJson}
      <div class="json-header">
        <Pager
          i18n={i18nLocal}
          expandUp={true}
          {totalItems}
          showPageSize={false}
          showQuickNav={false}
          showNav={showPagerNav}
          value={{
            page,
            pageSize,
          }}
          hasBoundaryRight={true}
          on:change={onPage}
        />
      </div>
      <div class="json">
        {#if paginatedData}
          <div><JSONTree data={paginatedData} /></div>
        {:else}
          <div class="loading">Loading...</div>
        {/if}
      </div>
      {#if showPagerSize}
        <div class="json-footer">
          <Pager
            i18n={i18nLocal}
            expandUp={true}
            {totalItems}
            showPageSize={showPagerSize}
            showQuickNav={showPagerNav}
            showNav={showPagerNav}
            value={{
              page,
              pageSize,
            }}
            hasBoundaryRight={true}
            on:change={onPage}
          />
        </div>
      {/if}
    {:else if isMerkleProof}
      <div class="merkle-proof">
      </div>
    {/if}
  </div>
</Card>

<style>
  .content {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
  }

  .tabs {
    display: flex;
    gap: 8px;
    width: 100%;

    padding-bottom: 32px;
    border-bottom: 1px solid #0a1018;
  }

  .json-header {
    box-sizing: var(--box-sizing);
    margin-top: 32px;
    width: 100%;
    display: flex;
    justify-content: flex-end;
  }

  .json {
    box-sizing: var(--box-sizing);
    margin-top: 16px;

    padding: 25px;
    border-radius: 10px;
    background: var(--app-bg-color);

    width: 100%;
    overflow-x: auto;
  }

  .json-footer {
    box-sizing: var(--box-sizing);
    margin-top: 16px;
    width: 100%;
    display: flex;
    justify-content: center;
  }

  .loading {
    color: rgba(255, 255, 255, 0.66);
    text-align: center;
    padding: 40px;
  }

  .merkle-proof {
    box-sizing: var(--box-sizing);
    margin-top: 32px;
    width: 100%;
  }


  .fields {
    box-sizing: var(--box-sizing);
    margin-top: 32px;

    display: grid;
    grid-template-columns: 1fr 1fr;
    column-gap: 16px;
    row-gap: 10px;

    width: 100%;
  }
  .fields.collapse {
    grid-template-columns: 1fr;
  }

  .entry {
    display: grid;
    grid-template-columns: 1fr 2fr;
    column-gap: 16px;
    row-gap: 16px;

    width: 100%;
    padding-bottom: 10px;
  }
  .entry:last-child {
    padding-bottom: 0;
  }

  .label {
    color: rgba(255, 255, 255, 0.66);
    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .value {
    word-break: break-all;

    color: rgba(255, 255, 255, 0.88);
    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .copy-link {
    display: flex;
    word-break: break-all;
  }
  .icon {
    padding-top: 4px;
    padding-left: 8px;
    cursor: pointer;
  }

  button.icon {
    background: none;
    border: none;
    color: inherit;
    font: inherit;
  }
</style>
