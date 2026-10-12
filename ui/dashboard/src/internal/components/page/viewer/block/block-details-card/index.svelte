<svelte:options runes={true} />

<script lang="ts">
  import { goto } from '$app/navigation'
  import { tippy } from '$lib/stores/media'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { getDetailsUrl, DetailType, DetailTab, reverseHashParam } from '$internal/utils/urls'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'

  import { Button, Icon } from '$lib/components'
  import { getDifficultyFromBits } from '$lib/utils/difficulty'
  import { addNumCommas, dataSize, formatDate, formatNumberExpStr } from '$lib/utils/format'
  import JSONTree from '$internal/components/json-tree/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'
  import { getItemApiUrl, ItemType } from '$internal/api'
  import { blockHashToMiner } from '$internal/stores/p2pStore'

  const baseKey = 'page.viewer-block.details'
  const fieldKey = `${baseKey}.fields`

  const t = $derived($i18n.t)

  const collapse = $derived($mediaSize < MediaSize.sm)

  let {
    data = {},
    display = DetailTab.overview,
    ondisplay,
  }: {
    data?: any
    display?: DetailTab
    ondisplay?: (detail: { value: string }) => void
  } = $props()

  const expandedHeader = $derived(data?.expandedHeader)
  const isOverview = $derived(display === DetailTab.overview)
  const isJson = $derived(display === DetailTab.json)

  // Populate the block hash -> miner cache when block data is loaded
  $effect(() => {
    if (expandedHeader?.hash && expandedHeader?.miner) {
      blockHashToMiner.update(map => {
        map.set(expandedHeader.hash, expandedHeader.miner)
        // Keep cache size manageable (same limit as in p2pStore)
        if (map.size > 1000) {
          const firstKey = map.keys().next().value
          if (firstKey !== undefined) {
            map.delete(firstKey)
          }
        }
        return map
      })
    }
  })

  function onDisplay(value) {
    ondisplay?.({ value })
  }

  function onReverseHash(hash) {
    reverseHashParam(hash)
  }

  function navToBlock(hash) {
    if (hash) {
      goto(getDetailsUrl(DetailType.block, hash))
    }
  }

  const difficultyDisplay = $derived(formatNumberExpStr(getDifficultyFromBits(expandedHeader.bits)))
</script>

<Card title={t(`${baseKey}.title`, { height: expandedHeader.height })}>
  {#snippet subtitle()}
  <div class="copy-link">
    <div class="hash">{expandedHeader.hash}</div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-duplicate-line"
        action={copyTextToClipboardVanilla}
        actionData={expandedHeader.hash}
        size={15}
      />
    </div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-url-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-bracket-line"
        action={copyTextToClipboardVanilla}
        actionData={getItemApiUrl(ItemType.block, expandedHeader.hash)}
        size={15}
      />
    </div>
    <button
      class="icon"
      onclick={() => onReverseHash(expandedHeader.hash)}
      use:$tippy={{ content: t('tooltip.reverse-hash') }}
      type="button"
    >
      <Icon name="icon-reeverse-line" size={15} />
    </button>
    &nbsp;&nbsp;&nbsp;
    <a href="/forks/?hash={expandedHeader.hash}">forks</a>
  </div>
  {/snippet}
  {#snippet headerTools()}
  <div class="btns">
    <Button
      size="small"
      icon="icon-chevron-left-line"
      ico={true}
      disabled={!expandedHeader.previousblockhash}
      tooltip={expandedHeader.previousblockhash ? t('tooltip.previous-block') : ''}
      onclick={() => navToBlock(expandedHeader.previousblockhash)}
    />
    <Button
      size="small"
      icon="icon-chevron-right-line"
      ico={true}
      disabled={!data?.nextblock}
      tooltip={data?.nextblock ? t('tooltip.next-block') : ''}
      onclick={() => navToBlock(data?.nextblock)}
    />
  </div>
  {/snippet}
  <div class="content">
    <div class="tabs">
      <Button
        size="medium"
        hasFocusRect={false}
        selected={isOverview}
        variant={isOverview ? 'tertiary' : 'primary'}
        onclick={() => onDisplay('overview')}>{t(`${baseKey}.tab.overview`)}</Button
      >
      <Button
        size="medium"
        hasFocusRect={false}
        selected={isJson}
        variant={isJson ? 'tertiary' : 'primary'}
        onclick={() => onDisplay('json')}>{t(`${baseKey}.tab.json`)}</Button
      >
    </div>
    {#if isOverview}
      <div class="fields" class:collapse>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.timestamp`)}</div>
            <div class="value">{formatDate(expandedHeader.time * 1000)}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.txCount`)}</div>
            <div class="value">{addNumCommas(expandedHeader.txCount)}</div>
          </div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.totalFee`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.avgFee`)}</div>
            <div class="value">TBD</div>
          </div> -->
          <div class="entry">
            <div class="label">{t(`${fieldKey}.sizeInBytes`)}</div>
            <div class="value">
              {dataSize(expandedHeader.sizeInBytes)}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.difficulty`)}</div>
            <div class="value">
              {@html difficultyDisplay}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.nonce`)}</div>
            <div class="value">{expandedHeader.nonce}</div>
          </div>
        </div>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.bits`)}</div>
            <div class="value">{expandedHeader.bits}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.confirmations`)}</div>
            <div class="value">
              {addNumCommas(data.latestBlockData.height - expandedHeader.height)}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.merkleroot`)}</div>
            <div class="value">{expandedHeader.merkleroot}</div>
          </div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.chainwork`)}</div>
            <div class="value">TBD</div>
          </div> -->
          <div class="entry">
            <div class="label">{t(`${fieldKey}.miner`)}</div>
            <div class="value">{expandedHeader.miner || '-'}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.invalid`)}</div>
            <div class="value">{expandedHeader.invalid ? 'Yes' : 'No'}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.processed_at`)}</div>
            <div class="value">{expandedHeader.processed_at ? formatDate(new Date(expandedHeader.processed_at).getTime()) : '-'}</div>
          </div>
        </div>
      </div>
    {:else if isJson}
      <div class="json">
        <div><JSONTree {data} blockHash={expandedHeader.hash} /></div>
      </div>
    {/if}
  </div>
</Card>

<style>
  .btns {
    display: flex;
    gap: 4px;
  }

  .content {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
  }

  .tabs {
    display: flex;
    gap: 8px;
    width: 100%;

    padding-bottom: 16px;
    border-bottom: 1px solid var(--app-bg-color);
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

  .fields {
    box-sizing: var(--box-sizing);
    margin-top: 16px;

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
    color: var(--comp-label-color);
    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px;
    letter-spacing: 0.3px;
  }

  .value {
    word-break: break-all;

    color: var(--app-color);
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
