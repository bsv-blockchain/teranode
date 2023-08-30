<script>
	export let block = {};

	function formatNumber(number) {
		return new Intl.NumberFormat().format(number);
	}
</script>

<td class="has-text-right">{block.height}</td>
<td>{new Date(block.timestamp).toISOString().replace('T', ' ')}</td>
<td class="has-text-right">{block.age}</td>
<td class="has-text-right">{block.deltaTime}</td>
<td>{'miner'}</td>
<td class="has-text-right">{formatNumber(block.coinbaseValue)}</td>
<td class="has-text-right">{block.transactionCount}</td>
<td class="has-text-right">{block.size}</td>
