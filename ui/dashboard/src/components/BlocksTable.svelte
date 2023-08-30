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
		{#each blocks as block (block.height + block.hash)}
			<tr>
				<BlocksTableRow {block} />
			</tr>
		{/each}
	</tbody>
</table>

<style>
	.table {
		width: 100%;
	}

	.table tr th {
		text-align: center;
	}
	.table tr:nth-child(even) {
		background-color: #f9f9f9; /* Zebra-striping for even rows */
	}
</style>
