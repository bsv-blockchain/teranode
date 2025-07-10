<script lang="ts">
  import { formatSatoshi } from '$lib/utils/format'
  import { getDetailsUrl, DetailType } from '$internal/utils/urls'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import { tippy } from '$lib/stores/media'
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'

  const baseKey = 'page.viewer-block.coinbase'
  const fieldKey = `${baseKey}.fields`

  $: t = $i18n.t
  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = {}

  $: coinbaseTx = data?.coinbase_tx
  $: blockHeight = data?.expandedHeader?.height
  $: hasOutputs = coinbaseTx?.outputs?.length > 0

  // Calculate total block reward from coinbase outputs
  $: totalReward = coinbaseTx?.outputs?.reduce((sum, output) => sum + (output.satoshis || 0), 0) || 0
  
  // Calculate transaction size from hex
  $: txSize = coinbaseTx?.hex ? coinbaseTx.hex.length / 2 : 0
</script>

{#if coinbaseTx}
  <Card title={t(`${baseKey}.title`)} headerPadding="20px 24px 16px 24px">
    <div class="copy-link" slot="subtitle">
      <a href={getDetailsUrl(DetailType.tx, coinbaseTx.txid)} class="hash-link">{coinbaseTx.txid}</a>
      <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
        <ActionStatusIcon
          icon="icon-duplicate-line"
          action={copyTextToClipboardVanilla}
          actionData={coinbaseTx.txid}
          size={15}
        />
      </div>
    </div>
    <div class="content">
      <div class="fields" class:collapse>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.blockReward`)}</div>
            <div class="value">{formatSatoshi(totalReward)} BSV</div>
          </div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.outputCount`)}</div>
            <div class="value">{coinbaseTx.outputs?.length || 0}</div>
          </div>
        </div>
        <div>
          <div class="entry">
            <div class="label">{t(`${fieldKey}.size`)}</div>
            <div class="value">{txSize} bytes</div>
          </div>
        </div>
      </div>
    </div>
  </Card>
{/if}

<style>
  .content {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
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

  .link {
    color: #4a9eff;
    text-decoration: none;
  }
  .link:hover {
    text-decoration: underline;
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
  .hash-link {
    color: #4a9eff;
    text-decoration: none;
  }
  .hash-link:hover {
    text-decoration: underline;
  }
</style>