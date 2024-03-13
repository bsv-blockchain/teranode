<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { formatSatoshi } from '$lib/utils/format'

  import { DetailType, getHashLinkProps } from '$internal/utils/urls'
  import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'
  import i18n from '$internal/i18n'

  const baseKey = 'page.viewer-tx.txs'

  $: t = $i18n.t

  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = []

  let sliceCount = 10

  function increaseSlize() {
    sliceCount += 10
  }

  $: inputSlice = data.inputs.slice(0, sliceCount)
  $: outputSlice = data.outputs.slice(0, sliceCount)
</script>

<div class="io" class:collapse>
  <div class="col">
    <div class="title">
      <div>{t(`${baseKey}.input.title`, { count: data.inputs.length })}</div>
      <div class="total">
        {t(`${baseKey}.input.total`, {
          amount: formatSatoshi(data.inputs.reduce((acc, item) => (acc += item.vout), 0)),
        })}
      </div>
    </div>
    <div class="items">
      {#each inputSlice as input, i}
        <div class="entry">
          <div class="index">
            {i}
          </div>
          <div class="values">
            <div class="copy-link">
              <LinkHashCopy {...getHashLinkProps(DetailType.tx, input.txid, t, false)} />
            </div>
            <span>{`${input.previousTxSatoshis ? formatSatoshi(input.previousTxSatoshis) : '-'} BSV`}</span>
          </div>
        </div>
      {/each}
    </div>
    {#if data.inputs.length > inputSlice.length}
      <div class="load-more" on:click={increaseSlize}>{t(`${baseKey}.load-more`)}</div>
    {/if}
  </div>
  <div class="col">
    <div class="title">
      <div>{t(`${baseKey}.output.title`, { count: data.outputs.length })}</div>
      <div class="total">
        {t(`${baseKey}.output.total`, {
          amount: formatSatoshi(data.outputs.reduce((acc, item) => (acc += item.satoshis), 0)),
        })}
      </div>
    </div>
    <div class="items">
      {#each outputSlice as output, i}
        <div class="entry">
          <div class="index">
            {i}
          </div>
          <div class="values">
            <div class="copy-link">
              <LinkHashCopy {...getHashLinkProps(DetailType.tx, output.lockingScript, t, false)} />
            </div>
            <span>{`${formatSatoshi(output.satoshis)} BSV`}</span>
          </div>
        </div>
      {/each}
    </div>
    {#if data.outputs.length > outputSlice.length}
      <div class="load-more" on:click={increaseSlize}>{t(`${baseKey}.load-more`)}</div>
    {/if}
  </div>
</div>

<style>
  .io {
    box-sizing: var(--box-sizing);

    padding: 16px 0;
    min-height: 200px;
    width: 100%;

    display: grid;
    grid-template-columns: 1fr 1fr;
    column-gap: 16px;
    row-gap: 10px;

    /* border-top: 1px solid rgba(255, 255, 255, 0.08); */
    /* border-bottom: 1px solid rgba(255, 255, 255, 0.08); */
  }
  .io.collapse {
    grid-template-columns: 1fr;
  }
  .col:first-child {
    border-right: 1px solid rgba(255, 255, 255, 0.08);
    padding-right: 10px;
  }
  .io.collapse .col:first-child {
    border-right: none;
    padding: 0 0 20px 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.08);
  }

  .col {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  .title {
    display: flex;
    align-items: center;
    justify-content: space-between;

    color: rgba(255, 255, 255, 0.88);

    font-family: Satoshi;
    font-size: 17px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
    letter-spacing: 0.34px;

    padding: 10px 24px;
  }

  .title .total {
    color: rgba(255, 255, 255, 0.88);

    text-align: right;
    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 700;
    line-height: 18px;
    letter-spacing: 0.26px;
  }

  .items {
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  .entry {
    display: flex;
    align-items: flex-start;
    padding: 0 24px;

    color: rgba(255, 255, 255, 0.88);

    font-family: Satoshi;
    font-size: 15px;
    font-style: normal;
    font-weight: 400;
    line-height: 24px;
    letter-spacing: 0.3px;

    word-break: break-all;
  }

  .index {
    width: 40px;
  }
  .copy-link {
    display: flex;
  }

  .values {
    display: flex;
    flex-direction: column;
  }

  .load-more {
    color: #1778ff;
    font-weight: 700;
    cursor: pointer;

    padding: 16px 24px 0 24px;
  }
</style>
