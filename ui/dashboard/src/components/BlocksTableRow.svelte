<script>
  export let block = {}

  function formatNumber(number) {
    return new Intl.NumberFormat().format(number)
  }
</script>

<td>
  <a class="has-text-right" href={`/viewer/header/${block.hash}`}>
    {block.height}
  </a>
</td>
<td>
  <a class="has-text-right" href={`/viewer/block/${block.hash}`}>
    {block.hash.substring(0, 8)}
  </a>
</td>

<td>{new Date(block.timestamp).toISOString().replace('T', ' ')}</td>
<td class="has-text-right">{block.age}</td>
<td class="has-text-right">{block.deltaTime}</td>
<td>{block.miner}</td>
<td class="has-text-right">{formatNumber(block.coinbaseValue)}</td>
<td class="has-text-right">{formatNumber(block.transactionCount)}</td>
<td class="has-text-right">{formatNumber(block.transactionCount / block.timeDiff)}</td>
<td class="has-text-right">{formatNumber(block.size)}</td>
