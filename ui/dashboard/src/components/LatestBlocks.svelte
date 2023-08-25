<script>
	import { blocks, error } from '@stores/nodeStore.js';
	import LatestBlocksNode from '@components/LatestBlocksNode.svelte';
</script>

<div>
	{#if $error}
		<p>Error fetching hashes: {$error}</p>
	{:else}
		{#each Object.entries($blocks) as [key, item]}
			<p>{key}</p>
			{#each item as block}
				<LatestBlocksNode data={block} />
			{/each}
		{/each}
	{/if}
</div>
