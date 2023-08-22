<script>
	import Node from './Node.svelte';
	export let nodes;
	export let error;
</script>

<div class="card panel">
	<header class="card-header">
		<p class="card-header-title">Chaintip tracker</p>
	</header>
	<div class="card-content">
		{#if error}
			<p>Error fetching chaintip data: {error}</p>
		{:else}
			<table class="table">
				<thead>
					<tr>
						<th>Source</th>
						<th>Name</th>
						<th>Address</th>
						<th>Height</th>
						<th>Latest hash</th>
						<th>Previous hash</th>
					</tr>
				</thead>
				<tbody>
					{#each nodes as node (node.ip)}
						{#if node.blobServerHTTPAddress}
							<Node name={node.source || 'anonymous'} address={node.blobServerHTTPAddress} />
						{/if}
					{/each}
				</tbody>
			</table>
		{/if}
	</div>
	<!-- <footer class="card-footer">
		<a class="card-footer-item">Action 1</a>
		<a class="card-footer-item">Action 2</a>
	</footer> -->
</div>

<style>
	.panel {
		margin: 20px; /* Adjust as needed */
	}

	/* Custom styles for the table inside card-content */
	.card-content .table {
		width: 100%; /* Make the table take the full width of the card */
		border-collapse: collapse; /* Collapse table borders */
	}

	.card-content .table th,
	.card-content .table th {
		background-color: #f5f5f5; /* Light background for table headers */
		text-align: left; /* Align header text to the left */
	}

	.card-content .table tr:nth-child(even) {
		background-color: #f9f9f9; /* Zebra-striping for even rows */
	}
</style>
