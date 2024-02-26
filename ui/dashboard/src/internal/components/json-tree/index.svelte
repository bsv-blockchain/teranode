<script lang="ts">
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import { getDetailsUrl, DetailType } from '$internal/utils/urls'
  import { tippy } from '$lib/stores/media'

  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'

  import i18n from '$internal/i18n'

  $: t = $i18n.t

  export let level = 0 // used internally to set recursive level
  export let data = {}
  export let parentKey = ''
  export let blockHash = ''

  function getType(value: any) {
    if (Array.isArray(value)) return 'array'
    return typeof value
  }

  function castToArray(value: any): any[] {
    return value as any[]
  }
</script>

{#if data}
  {#if level === 0}
    <div class="tools">
      <div class="icon" use:$tippy={{ content: t('tooltip.copy-json-to-clipboard') }}>
        <ActionStatusIcon
          icon="icon-duplicate-line"
          action={copyTextToClipboardVanilla}
          actionData={JSON.stringify(data, null, 2)}
          size={15}
        />
      </div>
    </div>
  {/if}
  <div class="json-tree">
    {#if typeof data === 'object'}
      &#123;
      <ul>
        {#each Object.entries(data) as [key, value]}
          <li>
            <span class="key">{key}:</span>
            {#if getType(value) === 'object' && value !== null}
              <svelte:self data={value} {blockHash} level={level + 1} />
            {:else if getType(value) === 'array'}
              [
              <ul>
                {#each value as item (item)}
                  <li><svelte:self data={item} parentKey={key} {blockHash} level={level + 1} /></li>
                {/each}
              </ul>
              ]
            {:else if getType(value) === 'string'}
              {#if value.length === 64}
                {#if key.toLowerCase().includes('txid')}
                  <a href={getDetailsUrl(DetailType.tx, value)}>"{value}"</a>
                {:else if key.includes('block') || key === 'hash'}
                  <a href={getDetailsUrl(DetailType.block, value)}>"{value}"</a>
                {:else if key === 'utxoHash'}
                  <a href={getDetailsUrl(DetailType.utxo, value)}>"{value}"</a>
                {:else}
                  <span class="string">"{value}"</span>
                {/if}
              {:else}
                <span class="string">"{value}"</span>
              {/if}
            {:else if getType(value) === 'number'}
              <span class="string2">{value}</span>
            {:else}
              <span class={getType(value)}>{value}</span>
            {/if}
          </li>
        {/each}
      </ul>
      &#125;
    {:else if castToArray(data).length === 64 && parentKey === 'subtrees'}
      <a href={getDetailsUrl(DetailType.subtree, `${data}`, blockHash ? { blockHash } : {})}
        >{data}</a
      >
    {:else if castToArray(data).length === 64 && parentKey.includes('block')}
      <a href={getDetailsUrl(DetailType.block, `${data}`)}>{data}</a>
    {:else if castToArray(data).length === 64 && parentKey.includes('utxo')}
      <a href={getDetailsUrl(DetailType.utxo, `${data}`)}>{data}</a>
    {:else if castToArray(data).length === 64 && parentKey.includes('parentTx')}
      <a href={getDetailsUrl(DetailType.tx, `${data}`)}>{data}</a>
    {:else}
      <span class={getType(data)}>{data}</span>
    {/if}
  </div>
{/if}

<style>
  .json-tree {
    font-family: var(--font-family-mono);
    font-size: 13px;
    font-style: normal;
    font-weight: 200;
    line-height: 20px;
  }

  .tools {
    display: flex;
    justify-content: flex-end;
    margin-bottom: -15px;
  }
  .tools .icon {
    color: rgba(255, 255, 255, 0.66);
    cursor: pointer;
  }

  ul {
    list-style-type: none;
    padding-left: 15px;
  }

  .key {
    color: rgba(255, 255, 255, 0.8);
  }

  .string {
    color: #15b241;
  }

  .string2 {
    color: #9917ff;
  }

  .boolean {
    color: blue;
  }

  .undefined,
  .null {
    color: gray;
  }
</style>
