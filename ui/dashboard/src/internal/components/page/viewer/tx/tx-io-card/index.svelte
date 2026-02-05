<script lang="ts">
  import { mediaSize, MediaSize } from '$lib/stores/media'
  import { formatSatoshi } from '$lib/utils/format'

  import { DetailType, getHashLinkProps } from '$internal/utils/urls'
  import LinkHashCopy from '$internal/components/item-renderers/link-hash-copy/index.svelte'
  import i18n from '$internal/i18n'
  import { detectScriptType, scriptToAsm, extractOpReturnData, getScriptTypeDescription, ScriptType, extractAddress } from '$internal/utils/bitcoin-scripts'
  import { onMount } from 'svelte'
  import * as api from '$internal/api'

  const baseKey = 'page.viewer-tx.txs'

  $: t = $i18n.t

  $: collapse = $mediaSize < MediaSize.sm

  export let data: any = []

  let sliceCount = 10
  let outputViewModes: { [key: number]: 'default' | 'asm' | 'hex' } = {}
  let inputViewModes: { [key: number]: 'default' | 'hex' } = {}
  let outputAddresses: { [key: number]: string | null } = {}

  let legacyPubKeyHashAddrID = 0x00
  let legacyScriptHashAddrID = 0x05

  function increaseSlize() {
    sliceCount += 10
  }

  function toggleOutputView(index: number) {
    const current = outputViewModes[index] || 'default'
    const modes = ['default', 'asm', 'hex'] as const
    const currentIndex = modes.indexOf(current)
    outputViewModes[index] = modes[(currentIndex + 1) % modes.length]
    outputViewModes = { ...outputViewModes } // Trigger reactivity
  }

  function toggleInputView(index: number) {
    const current = inputViewModes[index] || 'default'
    inputViewModes[index] = current === 'default' ? 'hex' : 'default'
    inputViewModes = { ...inputViewModes } // Trigger reactivity
  }

  $: inputSlice = data.inputs.slice(0, sliceCount)
  $: outputSlice = data.outputs.slice(0, sliceCount)

  // Track which outputs we've already processed to avoid re-processing
  let processedOutputsHash = ''

  // Extract addresses for outputs when data changes
  $: {
    const currentHash = data.outputs ? JSON.stringify(data.outputs.map(o => o.lockingScript)) : ''
    if (currentHash && currentHash !== processedOutputsHash) {
      processedOutputsHash = currentHash
      extractOutputAddresses()
    }
  }

  onMount(async () => {
    const resp: any = await api.getChainParams()
    if (resp?.ok) {
      if (typeof resp.data?.legacyPubKeyHashAddrID === 'number') {
        legacyPubKeyHashAddrID = resp.data.legacyPubKeyHashAddrID
      }
      if (typeof resp.data?.legacyScriptHashAddrID === 'number') {
        legacyScriptHashAddrID = resp.data.legacyScriptHashAddrID
      }
    }


    if (data?.outputs?.length) {
      processedOutputsHash = ''
      extractOutputAddresses()
    }
  })

  async function extractOutputAddresses() {
    if (!data.outputs) return

    const newAddresses: { [key: number]: string | null } = {}

    for (let index = 0; index < data.outputs.length; index++) {
      const output = data.outputs[index]
      const scriptType = detectScriptType(output.lockingScript)
      if (scriptType === ScriptType.P2PKH || scriptType === ScriptType.P2SH) {
        const address = await extractAddress(output.lockingScript, scriptType, {
          p2pkh: legacyPubKeyHashAddrID,
          p2sh: legacyScriptHashAddrID,
        })
        if (address) {
          newAddresses[index] = address
        }
      }
    }

    // Update all addresses at once to trigger reactivity only once
    outputAddresses = newAddresses
  }
</script>

<div class="io" class:collapse>
  <div class="col">
    <div class="title">
      <div>{t(`${baseKey}.input.title`, { count: data.inputs.length })}</div>
      <div class="total">
        {t(`${baseKey}.input.total`, {
          amount: formatSatoshi(
            data.inputs.reduce((acc, item) => (acc += item.previousTxSatoshis || 0), 0),
          ),
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
            {#if input.txid && input.txid !== '0000000000000000000000000000000000000000000000000000000000000000'}
              <div class="copy-link">
                <LinkHashCopy {...getHashLinkProps(DetailType.tx, input.txid, t, false)} />
                <span class="output-ref">:{input.vout || 0}</span>
              </div>
            {:else}
              <div class="coinbase-input">Coinbase Input</div>
            {/if}
            <span
              >{`${input.previousTxSatoshis ? formatSatoshi(input.previousTxSatoshis) : '-'} BSV`}</span
            >
            {#if input.unlockingScript}
              <button 
                class="view-toggle"
                on:click={() => toggleInputView(i)}
                type="button"
              >
                {inputViewModes[i] === 'hex' ? 'Show Default' : 'Show Hex'}
              </button>
              {#if inputViewModes[i] === 'hex'}
                <div class="script-hex">{input.unlockingScript}</div>
              {/if}
            {/if}
          </div>
        </div>
      {/each}
    </div>
    {#if data.inputs.length > inputSlice.length}
      <button class="load-more" on:click={increaseSlize} type="button"
        >{t(`${baseKey}.load-more`)}</button
      >
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
        {@const scriptType = detectScriptType(output.lockingScript)}
        {@const viewMode = outputViewModes[i] || 'default'}
        <div class="entry">
          <div class="index">
            {i}
          </div>
          <div class="values">
            <div class="script-type">
              <span class="type-badge {scriptType.toLowerCase()}">{scriptType}</span>
              <span class="type-desc">{getScriptTypeDescription(scriptType)}</span>
            </div>
            {#if outputAddresses[i] && (scriptType === ScriptType.P2PKH || scriptType === ScriptType.P2SH)}
              <div class="address">
                <span class="address-label">Address:</span>
                <span class="address-value">{outputAddresses[i]}</span>
              </div>
            {/if}
            <span class="amount">{`${formatSatoshi(output.satoshis)} BSV`}</span>
            
            <button 
              class="view-toggle"
              on:click={() => toggleOutputView(i)}
              type="button"
            >
              {viewMode === 'default' ? 'Show Script' : viewMode === 'asm' ? 'Show Hex' : 'Show Default'}
            </button>
            
            {#if viewMode === 'asm'}
              <div class="script-asm">{scriptToAsm(output.lockingScript)}</div>
            {:else if viewMode === 'hex'}
              <div class="script-hex">{output.lockingScript}</div>
            {:else if scriptType === ScriptType.OP_RETURN}
              {@const opReturnData = extractOpReturnData(output.lockingScript)}
              {#if opReturnData}
                <div class="op-return-data">
                  <span class="data-label">Data:</span> {opReturnData}
                </div>
              {/if}
            {/if}
          </div>
        </div>
      {/each}
    </div>
    {#if data.outputs.length > outputSlice.length}
      <button class="load-more" on:click={increaseSlize} type="button"
        >{t(`${baseKey}.load-more`)}</button
      >
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
    background: none;
    border: none;
    font: inherit;
    display: block;
    width: 100%;
    text-align: left;
  }

  .script-type {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 4px;
  }

  .type-badge {
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 12px;
    font-weight: 700;
    text-transform: uppercase;
    background: rgba(255, 255, 255, 0.1);
  }

  .type-badge.p2pkh {
    background: rgba(23, 120, 255, 0.2);
    color: #1778ff;
  }

  .type-badge.p2sh {
    background: rgba(255, 193, 7, 0.2);
    color: #ffc107;
  }

  .type-badge.op_return {
    background: rgba(76, 175, 80, 0.2);
    color: #4caf50;
  }

  .type-badge.p2pk {
    background: rgba(156, 39, 176, 0.2);
    color: #9c27b0;
  }

  .type-badge.p2ms {
    background: rgba(255, 87, 34, 0.2);
    color: #ff5722;
  }

  .type-desc {
    font-size: 13px;
    color: rgba(255, 255, 255, 0.66);
  }

  .amount {
    font-weight: 500;
    margin: 4px 0;
  }

  .view-toggle {
    background: none;
    border: 1px solid rgba(255, 255, 255, 0.2);
    color: rgba(255, 255, 255, 0.66);
    padding: 4px 12px;
    border-radius: 4px;
    font-size: 12px;
    cursor: pointer;
    margin: 8px 0;
    transition: all 0.2s;
  }

  .view-toggle:hover {
    background: rgba(255, 255, 255, 0.1);
    color: rgba(255, 255, 255, 0.88);
  }

  .script-asm,
  .script-hex {
    font-family: 'Courier New', monospace;
    font-size: 12px;
    background: rgba(0, 0, 0, 0.3);
    padding: 8px;
    border-radius: 4px;
    word-break: break-all;
    margin-top: 8px;
    color: rgba(255, 255, 255, 0.88);
  }

  .op-return-data {
    background: rgba(76, 175, 80, 0.1);
    padding: 8px;
    border-radius: 4px;
    margin-top: 8px;
    word-break: break-all;
  }

  .data-label {
    font-weight: 700;
    color: #4caf50;
  }

  .output-ref {
    color: rgba(255, 255, 255, 0.66);
    margin-left: 2px;
  }

  .coinbase-input {
    color: #ffc107;
    font-weight: 500;
  }

  .address {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 8px 0;
    padding: 8px;
    background: rgba(23, 120, 255, 0.1);
    border-radius: 4px;
    font-family: 'Courier New', monospace;
    font-size: 13px;
  }

  .address-label {
    color: rgba(255, 255, 255, 0.66);
    font-weight: 500;
  }

  .address-value {
    color: #1778ff;
    word-break: break-all;
  }
</style>
