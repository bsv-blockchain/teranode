<script>
	import { onMount, onDestroy } from 'svelte';

	let nodes = [];
	let error = null;
	let intervalId;

	async function fetchData() {
		try {
			const response = await fetch('http://localhost:8099/nodes');

			if (!response.ok) {
				throw new Error(`HTTP error! Status: ${response.status}`);
			}

			nodes = await response.json();
		} catch (e) {
			error = e.message;
		}
	}

	onMount(() => {
		fetchData(); // Fetch data immediately when component is mounted
		intervalId = setInterval(fetchData, 5000); // Re-fetch every 5 seconds

		return () => clearInterval(intervalId); // Cleanup when component is destroyed
	});
</script>

<div class="card panel">
	<header class="card-header">
		<p class="card-header-title">Chaintip tracker</p>
	</header>
	<div class="card-content">Not implemented yet</div>
	<!-- <footer class="card-footer">
		<a class="card-footer-item">Action 1</a>
		<a class="card-footer-item">Action 2</a>
	</footer> -->
</div>

<style>
	.panel {
		margin: 20px; /* Adjust as needed */
	}
</style>
