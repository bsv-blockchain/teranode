<script>
	import { websocketStore } from '@stores/nodeStore.js';
	import JSONTree from '@components/JSONTree.svelte';

	import { onDestroy } from 'svelte';

	let websocketData = null;

	// Subscribe to the WebSocket store
	const unsubscribe = websocketStore.subscribe((data) => {
		console.log(data);
		websocketData = data;
	});

	// Unsubscribe when the component is destroyed
	onDestroy(() => {
		unsubscribe();
	});
</script>

<section class="section">
	<div class="result">
		{#if websocketData}
			<div class="data-box">
				<JSONTree data={websocketData} />
			</div>
		{:else}
			<div class="wait-message">Waiting for WebSocket data...</div>
		{/if}
	</div>
</section>

<style>
	.data-box {
		border: 1px solid #ccc;
		border-radius: 4px;
		padding: 10px;
		max-width: 100%;
		overflow: auto;
	}

	.result {
		margin-top: 20px;
	}

	.wait-message {
		color: #240d61;
	}
</style>
