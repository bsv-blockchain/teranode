<svelte:options runes={true} />

<script lang="ts">
  import { onMount } from 'svelte'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import { getDetailsUrl, DetailType } from '$internal/utils/urls'
  import Icon from '$lib/components/icon/index.svelte'
  import { tippy } from '$lib/stores/media'
  import { valueSet } from '$lib/utils/types'

  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'
  import Self from './index.svelte'

  import i18n from '$internal/i18n'

  const t = $derived($i18n.t)

  let {
    // internal, don't set manually
    level = 0, // used internally to set recursive level
    id = 'exp',
    isLastChild = false,
    expandState = $bindable({}), // mapping from "expand" block id's to boolean indicating whether the block should be collapsed
    // options
    inlineArr = true,
    inlineObj = true,
    showCommas = true,
    showFinalComma = false,
    data = {},
    parentKey = '',
    blockHash = '',
  }: {
    level?: number
    id?: string
    isLastChild?: boolean
    expandState?: Record<string, boolean>
    inlineArr?: boolean
    inlineObj?: boolean
    showCommas?: boolean
    showFinalComma?: boolean
    data?: any
    parentKey?: string
    blockHash?: string
  } = $props()

  let expIds: string[] = $state([])

  function getType(value: any) {
    if (Array.isArray(value)) return 'array'
    return typeof value
  }

  function castToArray(value: any): any[] {
    return value as any[]
  }

  const entries = $derived<[string, any][]>(typeof data === 'object' ? Object.entries(data) : [])

  function onExpClick(e) {
    const elId = e.srcElement.id
    expandState = {
      ...expandState,
      [elId]: valueSet(expandState[elId]) ? !expandState[elId] : false,
    }
  }

  function onBulkClose(value: any) {
    if (level > 0) {
      return
    }
    expandState = { ...expIds.reduce((acc, id) => ({ ...acc, [id]: value }), {}) }
    // let's keep the root object expanded
    expandState[id] = false
  }

  onMount(() => {
    if (level > 0) {
      return
    }
    const items = document.querySelectorAll('div.expand')

    for (const item of items) {
      expandState[item.id] = false
      expIds.push(item.id)
    }
  })
</script>

{#if data}
  <!-- render tools only in root component -->
  {#if level === 0}
    <div class="tools">
      <div class="icon" use:$tippy={{ content: t('tooltip.collapse-all') }}>
        <Icon name="icon-chevron-right-line" size={15} onclick={() => onBulkClose(true)} />
      </div>
      <div class="icon" use:$tippy={{ content: t('tooltip.expand-all') }}>
        <Icon name="icon-chevron-down-line" size={15} onclick={() => onBulkClose(false)} />
      </div>
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
  <!-- start recursive block -->
  <div class="json-tree" style:--block-indent={`${(level + 1) * -15}px`}>
    {#if typeof data === 'object'}
      {#if level === 0 || !inlineObj}
        <button
          class="expand"
          class:closed={expandState[id]}
          {id}
          onclick={onExpClick}
          type="button"
        >
          <div class="exp_icon"><Icon name="icon-chevron-down-line" size={11} /></div>
        </button>
        <span class="obj-start">&#123;</span>{#if expandState[id]}..&#125;{/if}
      {/if}
      {#if !expandState[id]}
        <ul>
          {#each entries as [key, value], i}
            <li>
              {#if getType(value) === 'array' || getType(value) === 'object'}
                {#if (getType(value) === 'array' && inlineArr) || (getType(value) === 'object' && value !== null && inlineObj)}
                  <button
                    class="expand obj-start"
                    class:closed={expandState[id + '_' + i]}
                    id={id + '_' + i}
                    style="padding-left:3px;"
                    onclick={onExpClick}
                    type="button"
                  >
                    <div class="exp_icon"><Icon name="icon-chevron-down-line" size={11} /></div>
                  </button>
                  <span class="key" style="margin-left:-9px;">{key}:</span>
                {:else}
                  <span class="key">{key}:</span>
                {/if}

                {#if getType(value) === 'array' && inlineArr}
                  &#91;{#if expandState[id + '_' + i]}..&#93;{#if showCommas},{/if}{/if}
                {/if}
                {#if getType(value) === 'object' && value !== null && inlineObj}
                  &nbsp;&#123;{#if expandState[id + '_' + i]}..&#125;{#if showCommas},{/if}{/if}
                {/if}
              {:else}
                <span class="key">{key}:</span>
              {/if}

              {#if getType(value) === 'object' && value !== null}
                {#if !expandState[id + '_' + i] || !inlineObj}
                  <Self
                    data={value}
                    {blockHash}
                    level={level + 1}
                    {showFinalComma}
                    isLastChild={i === entries.length - 1}
                    id={id + '_' + i}
                    bind:expandState />
                {/if}
              {:else if getType(value) === 'array'}
                {#if !expandState[id + '_' + i] || !inlineArr}
                  {#if !inlineArr}
                    <br />
                    <button
                      class="expand"
                      class:closed={expandState[id + '_' + i + '_']}
                      id={id + '_' + i + '_'}
                      style="margin-left:-15px;"
                      onclick={onExpClick}
                      type="button"
                    >
                      <div class="exp_icon"><Icon name="icon-chevron-down-line" size={11} /></div>
                    </button>
                    <span class="obj-start" style="margin-left:-9px;">&#91;</span
                    >{#if expandState[id + '_' + i + '_']}..&#93;{/if}
                  {/if}
                  {#if (!inlineArr && !expandState[id + '_' + i + '_']) || (inlineArr && !expandState[id + '_' + i])}
                    <ul>
                      {#each value as item, j (item)}
                        <li>
                          <Self
                            data={item}
                            parentKey={key}
                            {blockHash}
                            level={level + 2}
                            {showFinalComma}
                            isLastChild={j === entries.length - 1}
                            inlineObj={false}
                            id={id + '_' + i + '_' + j}
                            bind:expandState />
                        </li>
                      {/each}
                    </ul>
                    &#93;{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                  {/if}
                {/if}
              {:else if getType(value) === 'string'}
                {#if value.length === 64}
                  {#if key.toLowerCase().includes('txid')}
                    <a href={getDetailsUrl(DetailType.tx, value)}>"{value}"</a
                    >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                  {:else if key.includes('block') || key === 'hash'}
                    <a href={getDetailsUrl(DetailType.block, value)}>"{value}"</a
                    >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                  {:else if key === 'utxoHash'}
                    <a href={getDetailsUrl(DetailType.utxo, value)}>"{value}"</a
                    >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                  {:else}
                    <span class="string">"{value}"</span
                    >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                  {/if}
                {:else}
                  <span class="string">"{value}"</span
                  >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
                {/if}
              {:else if getType(value) === 'number'}
                <span class="string2">{value}</span
                >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
              {:else}
                <span class={getType(value)}>{value}</span
                >{#if showCommas && (showFinalComma || i < entries.length - 1)},{/if}
              {/if}
            </li>
          {/each}
        </ul>
        &#125;{#if showCommas && (showFinalComma || !isLastChild) && level > 0},{/if}
      {/if}
    {:else if castToArray(data).length === 64 && parentKey === 'subtrees'}
      <a href={getDetailsUrl(DetailType.subtree, `${data}`, blockHash ? { blockHash } : {})}
        >"{data}"</a
      >{#if showCommas && (showFinalComma || !isLastChild)},{/if}
    {:else if castToArray(data).length === 64 && parentKey.includes('block')}
      <a href={getDetailsUrl(DetailType.block, `${data}`)}>"{data}"</a
      >{#if showCommas && (showFinalComma || !isLastChild)},{/if}
    {:else if castToArray(data).length === 64 && parentKey.includes('utxo')}
      <a href={getDetailsUrl(DetailType.utxo, `${data}`)}>"{data}"</a
      >{#if showCommas && (showFinalComma || !isLastChild)},{/if}
    {:else if castToArray(data).length === 64 && parentKey.includes('parentTx')}
      <a href={getDetailsUrl(DetailType.tx, `${data}`)}>"{data}"</a
      >{#if showCommas && (showFinalComma || !isLastChild)},{/if}
    {:else}
      <span class={getType(data)}>{data}</span>{#if showCommas && showFinalComma},{/if}
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
    gap: 8px;
  }
  .tools .icon {
    color: var(--comp-label-color);
    cursor: pointer;
  }

  ul {
    list-style-type: none;
    padding-left: 15px;
  }

  .key {
    color: var(--app-color);
  }

  .string {
    color: var(--json-tree-string-color, #15b241);
  }

  .string2 {
    color: var(--json-tree-string2-color, #9917ff);
  }

  .boolean {
    color: var(--json-tree-boolean-color, #1a6bd4);
  }

  .undefined,
  .null {
    color: var(--comp-label-color);
  }

  .expand {
    display: inline-block;
    position: relative;
    left: var(--block-indent);
    color: var(--comp-label-color);
  }

  button.expand {
    background: none;
    border: none;
    padding: 0;
    font: inherit;
    cursor: pointer;
  }
  .expand:hover {
    cursor: pointer;
    color: var(--app-color);
  }
  .exp_icon {
    width: 11px;
    height: 11px;
    pointer-events: none;

    padding: 2px;
    border-radius: 4px;

    background-color: transparent;
    transform: rotate(0);

    transition:
      transform var(--easing-duration, 0.2s) var(--easing-function, ease-in-out),
      color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }
  .expand.closed .exp_icon {
    transform: rotate(-90deg);
  }
  .expand:hover .exp_icon {
    background-color: var(--app-subtle-bg-color);
  }

  .obj-start {
    margin-left: -24px;
  }
  .expand.obj-start {
    margin-left: -18px;
  }
</style>
