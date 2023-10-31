<script>
  export let data = {}
  export let parentKey = ''

  function getType(value) {
    if (Array.isArray(value)) return 'array'
    return typeof value
  }
</script>

{#if data}
  {#if typeof data === 'object'}
    &#123;
    <ul>
      {#each Object.entries(data) as [key, value]}
        <li>
          <span class="key">{key}:</span>
          {#if getType(value) === 'object' && value !== null}
            <svelte:self data={value} />
          {:else if getType(value) === 'array'}
            [
            <ul>
              {#each value as item (item)}
                <li><svelte:self data={item} parentKey={key} /></li>
              {/each}
            </ul>
            ]
          {:else if getType(value) === 'string'}
            {#if value.length === 64}
              {#if key.toLowerCase().includes('txid')}
                <a href="/viewer/tx/{value}">"{value}"</a>
              {:else if key.includes('block') || key === 'hash'}
                <a href="/viewer/block/{value}">"{value}"</a>
              {:else if key === 'utxoHash'}
                <a href="/viewer/utxo/{value}">"{value}"</a>
              {:else}
                <span class="string">"{value}"</span>
              {/if}
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
  {:else if data.length === 64 && parentKey === 'subtrees'}
    <a href="/viewer/subtree/{data}">{data}</a>
  {:else if data.length === 64 && parentKey.includes('block')}
    <a href="/viewer/block/{data}">{data}</a>
  {:else if data.length === 64 && parentKey.includes('utxo')}
    <a href="/viewer/utxo/{data}">{data}</a>
  {:else if data.length === 64 && parentKey.includes('parentTx')}
    <a href="/viewer/tx/{data}">{data}</a>
  {:else}
    <span class={getType(data)}>{data}</span>
  {/if}
{/if}

<style>
  ul {
    list-style-type: none;
    padding-left: 15px;
  }

  .key {
    color: darkblue;
  }

  .string {
    color: green;
  }

  .string2 {
    color: darkred;
  }

  .boolean {
    color: blue;
  }

  .undefined,
  .null {
    color: gray;
  }
</style>
