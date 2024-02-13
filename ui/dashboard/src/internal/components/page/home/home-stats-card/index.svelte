<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { addNumCommas } from '$lib/utils/format'
  import { failure } from '$lib/utils/notifications'
  import * as api from '$internal/api'
  import { Button, Icon } from '$lib/components'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'
  import { sock as p2pSock } from '$internal/stores/p2pStore'

  let loading = true
  let data = {}

  const baseKey = 'page.home.stats'
  const fieldKey = `${baseKey}.fields`

  $: t = $i18n.t

  // TODO, decide how many connections the "live" indication should depend on
  //   $: connected =
  //     $p2pSock !== null && $nodeSock !== null && $statusSock !== null && $bootstrapSock !== null

  $: connected = $p2pSock !== null

  const cols = [
    'block_count',
    'tx_count',
    'max_height',
    'avg_block_size',
    'avg_tx_count_per_block',
    'txns_per_second',
  ]

  async function getData() {
    try {
      // console.log('call api: search = ', searchValue)
      const result: any = await api.getBlockStats()
      if (result.ok) {
        data = {
          block_count: {
            id: 'block_count',
            icon: 'icon-cube-line',
            value: result.data.block_count,
          },
          tx_count: {
            id: 'tx_count',
            icon: 'icon-arrow-transfer-line',
            value: result.data.tx_count,
          },
          max_height: {
            id: 'max_height',
            icon: 'icon-network-line',
            value: result.data.max_height,
          },
          avg_block_size: {
            id: 'avg_block_size',
            icon: 'icon-scale-line',
            value: Math.round(result.data.avg_block_size),
          },
          avg_tx_count_per_block: {
            id: 'avg_tx_count_per_block',
            icon: 'icon-scale-line',
            value: Math.round(result.data.avg_tx_count_per_block),
          },
          txns_per_second: {
            id: 'txns_per_second',
            icon: 'icon-binoculars-line',
            value: Math.round(result.data.tx_count / 24 / 60 / 60),
          },
        }
      } else {
        failure(result.error.message)
      }
    } catch (err: any) {
      console.error(err)
      failure(err.message)
    } finally {
      loading = false
    }

    return false
  }
  $: colCount = $mediaSize <= MediaSize.md ? ($mediaSize <= MediaSize.xs ? 1 : 2) : 3

  $: getData()
</script>

<Card title={t(`${baseKey}.title`)} showFooter={false} headerPadding="20px 24px 10px 24px">
  <svelte:fragment slot="header-tools">
    <div class="live">
      <div class="live-icon" class:connected>
        <Icon name="icon-status-light-glow-solid" size={14} />
      </div>
      <div class="live-label">{t(`${baseKey}.live`)}</div>
    </div>
    <Button
      size="small"
      ico={true}
      icon="icon-refresh-line"
      tooltip={t('tooltip.refresh')}
      on:click={getData}
    />
  </svelte:fragment>
  <div class="content" style:--grid-template-columns={`repeat(${colCount}, 1fr)`}>
    {#if loading}
      <div class="block">
        <div class="block-content">
          <div class="fields">
            <div class="label">{t(`${fieldKey}.loading`)}</div>
          </div>
        </div>
      </div>
    {:else}
      {#each cols as colId, i}
        <div class="block">
          <div
            class="block-content"
            class:first={i % colCount === 0}
            class:last={i % colCount === colCount - 1}
          >
            <div class="icon">
              <Icon name={data[colId].icon} size={18} />
            </div>
            <div class="fields">
              <div class="label">
                {t(`${fieldKey}.${colId}`)}
              </div>
              <div class="value">
                {addNumCommas(data[colId].value)}
              </div>
            </div>
          </div>
        </div>
      {/each}
    {/if}
  </div>
</Card>

<style>
  .live {
    display: flex;
    align-items: center;
    gap: 4px;

    color: rgba(255, 255, 255, 0.66);

    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 700;
    line-height: 18px;
    letter-spacing: 0.26px;

    text-transform: uppercase;
  }
  .live-icon {
    color: #ce1722;
  }
  .live-icon.connected {
    color: #15b241;
  }
  .live-label {
    color: rgba(255, 255, 255, 0.66);
  }

  .content {
    background: #0a1018;

    display: grid;
    grid-template-columns: var(--grid-template-columns);

    grid-gap: 1px 0;
  }

  .block {
    width: 100%;
    height: 120px;

    display: flex;
    align-items: center;

    background: var(--comp-bg-color);
  }

  .block-content {
    width: 100%;

    display: flex;
    align-items: flex-start;
    gap: 8px;

    color: rgba(255, 255, 255, 0.66);
    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 700;
    line-height: 18px;
    letter-spacing: 0.26px;

    margin-right: 28px;
    padding: 14px 0;
    border-right: 1px solid #0a1018;
  }
  /* .block-content.first {
    background: red;
  } */
  .block-content.last {
    border-right: none;
  }

  .fields {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .label {
    text-transform: uppercase;
  }

  .value {
    color: rgba(255, 255, 255, 0.88);

    font-family: Satoshi;
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;
  }
</style>
