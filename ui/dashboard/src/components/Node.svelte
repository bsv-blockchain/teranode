<script>
	import { onMount } from 'svelte';

	export let name;
	export let source;
	export let address;
	let loading = true;
	let data = {};
	let error = null;
	let intervalId;

	async function fetchData() {
		try {
			const response = await fetch(`http://${address}/bestblockheader/json`);

			if (!response.ok) {
				throw new Error(`HTTP error! Status: ${response.status}`);
			}

			data = await response.json();
			error = null;
			loading = false;
		} catch (e) {
			error = e.message;
		}
	}

	onMount(() => {
		fetchData(); // Fetch data immediately when component is mounted
		intervalId = setInterval(fetchData, 2000); // Re-fetch every 2 seconds

		return () => clearInterval(intervalId); // Cleanup when component is destroyed
	});

	function shortHash(hash) {
		if (!hash) return '';

		return hash.substring(0, 8) + '...' + hash.substring(hash.length - 8);
	}
</script>

<tr>
	<td>{source}</td>
	<td>{name}</td>
	<td>{address}</td>
	<td>{data.height}</td>
	<td title={data.hash}>{shortHash(data.hash)}</td>
	<td title={data.previousblockhash}>{shortHash(data.previousblockhash)}</td>
</tr>

<style>
	td {
		border: 1px solid #e5e5e5; /* Add a light border to table cells */
		padding: 8px 12px; /* Add some padding to table cells */
	}
</style>
