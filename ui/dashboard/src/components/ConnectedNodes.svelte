<script>
	import { nodes, loading, error } from '@stores/nodeStore.js';
	import Spinner from '@components/Spinner.svelte';

	function humanTime(time) {
		let diff = new Date().getTime() - new Date(time).getTime();
		diff = diff / 1000;
		const days = Math.floor(diff / 86400);
		const hours = Math.floor((diff % 86400) / 3600);
		const minutes = Math.floor(((diff % 86400) % 3600) / 60);
		const seconds = Math.floor(((diff % 86400) % 3600) % 60);

		let difference = '';

		if (days > 0) {
			difference += days;
			difference += ' day' + (days === 1 ? ', ' : 's, ');
			difference += hours;
			difference += ' hour' + (hours === 1 ? ', ' : 's, ');
			difference += minutes;
			difference += ' minute' + (minutes === 1 ? ' and ' : 's and ');
			difference += seconds;
			difference += ' second' + (seconds === 1 ? '' : 's');
			return difference;
		}

		if (hours > 0) {
			difference += hours;
			difference += ' hour' + (hours === 1 ? ', ' : 's, ');
			difference += minutes;
			difference += ' minute' + (minutes === 1 ? ' and ' : 's and ');
			difference += seconds;
			difference += ' second' + (seconds === 1 ? '' : 's');
			return difference;
		}

		if (minutes > 0) {
			difference += minutes;
			difference += ' minute' + (minutes === 1 ? ' and ' : 's and ');
			difference += seconds;
			difference += ' second' + (seconds === 1 ? '' : 's');
			return difference;
		}

		if (seconds > 0) {
			difference += seconds;
			difference += ' second' + (seconds === 1 ? '' : 's');
			return difference;
		}

		return '0 seconds';
	}
</script>

<div class="card panel">
	<header class="card-header">
		<p class="card-header-title">Connected nodes</p>
	</header>
	<div class="card-content">
		{#if $error}
			<p>Error fetching nodes: {$error}</p>
		{:else if $nodes.length === 0}
			<p>No nodes connected</p>
			{#if $loading}
				<Spinner />
			{/if}
		{:else}
			<table class="table">
				<tbody>
					{#each $nodes as node (node.ip)}
						<tr>
							<td>{node.blobServerHTTPAddress || 'anonymous'}</td>
							<td>{node.source}</td>
							<td>{node.name}</td>
							<td>{node.ip}</td>
							<td>
								Connected at {new Date(node.connectedAt).toISOString().replace('T', ' ')}
								<br />
								<span class="small">{humanTime(node.connectedAt)} ago</span>
							</td>
						</tr>
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
	.table {
		width: 100%; /* Make the table take the full width of the card */
		border-collapse: collapse; /* Collapse table borders */
	}

	.table td {
		vertical-align: middle;
		padding-top: 2px;
		padding-bottom: 2px;
	}

	.table tr:nth-child(even) {
		background-color: #f9f9f9; /* Zebra-striping for even rows */
	}

	.small {
		font-size: 0.8em; /* Make the text smaller */
	}
</style>
