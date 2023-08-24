<script>
	import { blocks, error } from '@stores/nodeStore.js';
</script>

<div>
	{#if $error}
		<p>Error fetching hashes: {$error}</p>
	{:else}
		<pre>
	{JSON.stringify($blocks, null, 2)}
		</pre>
	{/if}
</div>
