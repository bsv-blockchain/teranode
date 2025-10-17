<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { tippy } from '$lib/stores/media'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { addNumCommas, dataSize } from '$lib/utils/format'
  import { DetailTab, reverseHashParam } from '$internal/utils/urls'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  import { Button, Icon } from '$lib/components'
  import JSONTree from '$internal/components/json-tree/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'
  import { getItemApiUrl, ItemType } from '$internal/api'

  const dispatch = createEventDispatcher()

  const baseKey = 'page.viewer-tx.details'
  const fieldKey = `${baseKey}.fields`

  $: t = $i18n.t

  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = {}
  export let display: DetailTab = DetailTab.overview

  $: isOverview = display === DetailTab.overview
  $: isJson = display === DetailTab.json
  $: isMerkleProof = display === DetailTab.merkleproof

  function onDisplay(value) {
    dispatch('display', { value })
  }

  function onReverseHash(hash) {
    reverseHashParam(hash)
  }
</script>

<Card title={t(`${baseKey}.title`, { height: data?.height })}>
  <div class="copy-link" slot="subtitle">
    <div class="hash">{data?.txid}</div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-duplicate-line"
        action={copyTextToClipboardVanilla}
        actionData={data?.txid}
        size={15}
      />
    </div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-url-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-bracket-line"
        action={copyTextToClipboardVanilla}
        actionData={getItemApiUrl(ItemType.tx, data?.txid)}
        size={15}
      />
    </div>
    <button
      class="icon"
      on:click={() => onReverseHash(data?.txid)}
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
      {#if (d?.blockHashes && d?.blockHashes.length > 0) || (d?.blockIDs && d?.blockIDs.length > 0)}
        <Button
          size="medium"
          hasFocusRect={false}
          selected={isMerkleProof}
          variant={isMerkleProof ? 'tertiary' : 'primary'}
          on:click={() => onDisplay('merkleproof')}>Merkle Proof</Button
        >
      {/if}
    </div>
    {#if isOverview}
      <div class="fields" class:collapse>
        <div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.block`)} FIX</div>
            <div class="value copy-link">
              {#if d?.blockHashes && d?.blockHashes.length > 0}
                <a href={`/viewer/block/${d?.blockHashes[0]}/`}>{d?.blockHashes[0]}</a>
                <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
                  <ActionStatusIcon
                    icon="icon-duplicate-line"
                    action={copyTextToClipboardVanilla}
                    actionData={d?.blockHashes[0]}
                    size={14}
                  />
                </div>
              {/if}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.timestamp`)}</div>
            <div class="value">TBD</div>
          </div> -->
          <div class="entry">
            <div class="label">{t(`${fieldKey}.sizeInBytes`)}</div>
            <div class="value">
              {dataSize(data?.sizeInBytes)}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.blockHeight`)}</div>
            <div class="value block-links">
              {#if data?.blockHeights && data?.blockHeights.length > 0 && data?.blockHashes && data?.blockHashes.length > 0}
                {#each data.blockHeights as height, i}
                  {#if data.blockHashes[i]}
                    <a href={`/viewer/block/?hash=${data.blockHashes[i]}`} class="block-link">
                      {height}
                    </a>
                    {#if i < data.blockHeights.length - 1}
                      <span>, </span>
                    {/if}
                  {:else}
                    {height}{#if i < data.blockHeights.length - 1}, {/if}
                  {/if}
                {/each}
              {:else if data?.blockHeights && data?.blockHeights.length > 0}
                {data.blockHeights.join(', ')}
              {:else}
                <span class="not-in-block">Not in block</span>
              {/if}
            </div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.subtree`)}</div>
            <div class="value subtree-info">
              {#if data?.subtreeIdxs && data?.subtreeIdxs.length > 0 && data?.subtreeHashes && data?.subtreeHashes.length > 0 && data?.blockHashes && data?.blockHashes.length > 0}
                {#each data.subtreeIdxs as subtreeIdx, i}
                  <div class="subtree-item">
                    {#if data.subtreeHashes[i]}
                      <a href={`/viewer/subtree/?hash=${data.subtreeHashes[i]}&blockHash=${data.blockHashes[i]}`} class="subtree-link">
                        Subtree #{subtreeIdx}
                      </a>
                    {:else}
                      Subtree #{subtreeIdx}
                    {/if}
                    {#if data.blockHashes[i]}
                      <span class="in-block">
                        in <a href={`/viewer/block/?hash=${data.blockHashes[i]}`} class="block-link">
                          Block #{data.blockHeights[i] || ''}
                        </a>
                      </span>
                    {/if}
                  </div>
                {/each}
              {:else if data?.subtreeIdxs && data?.subtreeIdxs.length > 0}
                {data.subtreeIdxs.map(subtreeIdx => `Subtree #${subtreeIdx}`).join(', ')}
              {:else}
                <span class="not-in-subtree">Not in a subtree</span>
              {/if}
            </div>
          </div>
        </div>
        <div>
          <!-- <div class="entry">
            <div class="label">{t(`${fieldKey}.confirmations`)}</div>
            <div class="value">TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.sizeInBytes`)}</div>
            <div class="value">{addNumCommas(d?.sizeInBytes)} B</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.fee_rate`)}</div>
            <div class="value">{d?.fee} BSV / KB - TBD</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.fee_paid`)}</div>
            <div class="value">TBD</div>
          </div> -->
          {#if data?.fee !== undefined}
            <div class="entry">
              <div class="label">{t(`${fieldKey}.fee`)}</div>
              <div class="value">{addNumCommas(data.fee)} satoshis</div>
            </div>
          {/if}
          {#if data?.lockTime !== undefined}
            <div class="entry">
              <div class="label">{t(`${fieldKey}.lockTime`)}</div>
              <div class="value">{data.lockTime}</div>
            </div>
          {/if}
          {#if data?.isCoinbase !== undefined}
            <div class="entry">
              <div class="label">{t(`${fieldKey}.type`)}</div>
              <div class="value">{data.isCoinbase ? 'Coinbase' : 'Regular'}</div>
            </div>
          {/if}
        </div>
      </div>
    {:else if isJson}
      <div class="json">
        <div><JSONTree {data} /></div>
      </div>
    {:else if isMerkleProof}
      <div class="merkle-proof">
        <slot name="merkle-proof" />
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
    column-gap: 50px;
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

  .block-links {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
  }

  .block-link {
    color: #1778ff;
    text-decoration: none;
    transition: opacity 0.2s;
  }

  .block-link:hover {
    opacity: 0.8;
    text-decoration: underline;
  }

  .subtree-link {
    color: #1778ff;
    text-decoration: none;
    transition: opacity 0.2s;
    font-weight: 500;
  }

  .subtree-link:hover {
    opacity: 0.8;
    text-decoration: underline;
  }

  .subtree-info {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .subtree-item {
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .in-block {
    color: rgba(255, 255, 255, 0.66);
    font-size: 14px;
  }

  .not-in-block,
  .not-in-subtree {
    color: rgba(255, 255, 255, 0.4);
    font-style: italic;
  }
</style>
