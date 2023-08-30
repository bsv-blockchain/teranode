<script>
	import BlocksTableRow from '@components/BlocksTableRow.svelte';

	export let blocks = [];

	// blocks = blocks.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
</script>

<table class="table">
	<thead>
		<tr>
			<th>Height</th>
			<th>Timestamp (UTC)</th>
			<th>Age</th>
			<th>Delta Time</th>
			<th>Miner</th>
			<th>Fees</th>
			<th>Number of Transactions</th>
			<th>Size</th>
		</tr>
	</thead>
	<tbody>
		{#each blocks as block (block.height)}
			<BlocksTableRow {block} />
		{/each}
	</tbody>
</table>

<style>
	.table {
		width: 100%;
	}
</style>
