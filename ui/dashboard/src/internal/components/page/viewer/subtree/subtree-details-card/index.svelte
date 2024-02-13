<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  // import { goto } from '$app/navigation'
  import { tippy } from '$lib/stores/media'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'

  import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'
  import {
    getDetailsUrl,
    DetailType,
    DetailTab,
    reverseHashParam,
    getHashLinkProps,
  } from '$internal/utils/urls'

  import { Button, Icon } from '$lib/components'
  import JSONTree from '$internal/components/json-tree/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'

  const dispatch = createEventDispatcher()

  const baseKey = 'page.viewer-subtree.details'
  const fieldKey = `${baseKey}.fields`

  $: t = $i18n.t

  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = {}
  export let display: DetailTab = DetailTab.overview

  $: expandedData = data?.expandedData
  $: isOverview = display === DetailTab.overview
  $: isJson = display === DetailTab.json

  function onDisplay(value) {
    dispatch('display', { value })
  }

  function onReverseHash(hash) {
    reverseHashParam(hash)
  }

  // function navToSubtree(hash) {
  //   if (hash) {
  //     goto(getDetailsUrl(DetailType.subtree, hash))
  //   }
  // }
</script>

<Card title={t(`${baseKey}.title`, { height: expandedData.height })}>
  <div class="copy-link" slot="subtitle">
    <div class="hash">{expandedData.hash}</div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-duplicate-line"
        action={copyTextToClipboardVanilla}
        actionData={expandedData.hash}
        size={15}
      />
    </div>
    <div
      class="icon"
      on:click={() => onReverseHash(expandedData.hash)}
      use:$tippy={{ content: t('tooltip.reverse-hash') }}
    >
      <Icon name="icon-reeverse-line" size={15} />
    </div>
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
    </div>
    {#if isOverview}
      <div class="fields" class:collapse>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.block`)}</div>
            <div class="value">
              {#if data?.expandedBlockData}
                <LinkHashCopy
                  {...getHashLinkProps(DetailType.block, data?.expandedBlockData.hash, t)}
                />
              {:else}
                {t('data.not_available')}
              {/if}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.txCount`)}</div>
            <div class="value">{expandedData.transactionCount}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.totalFee`)}</div>
            <div class="value">{expandedData.fee}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.avgFee`)}</div>
            <div class="value">{expandedData.fee / expandedData.transactionCount}</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.sizeInBytes`)}</div>
            <div class="value">{t('unit.value.kb', { value: expandedData.size / 1000 })}</div>
          </div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.nonce`)}</div>
            <div class="value">TBD</div>
          </div> -->
        </div>
        <div>
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
      <div class="json">
        <div><JSONTree {data} /></div>
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

  .json {
    box-sizing: var(--box-sizing);
    margin-top: 32px;

    padding: 25px;
    border-radius: 10px;
    background: var(--app-bg-color);

    width: 100%;
    overflow-x: auto;
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
</style>
