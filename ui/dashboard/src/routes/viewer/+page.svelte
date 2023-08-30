<script>
	import JSONTree from '@components/JSONTree.svelte';

	let hash = '';
	let type = '';
	let data = null;
	let addr = 'https://miner1.ubsv.dev:8090';
	let error = null;
	let url = '';

	$: if (addr && type && hash && hash.length === 64) {
		url = addr + '/' + type + '/' + hash + '/json';
	} else {
		url = '';
	}

	const itemTypes = ['tx', 'subtree', 'header', 'block', 'utxo'];

	async function fetchData() {
		try {
			const response = await fetch(url);
			if (!response.ok) {
				throw new Error(`HTTP error! Status: ${response.status}`);
			}

			const d = await response.json();
			d.extra = {
				name: 'Simon',
				age: 44
			};

			data = d;
		} catch (err) {
			error = err.message;
		}
	}
</script>

<section class="search-bar">
	<div class="search-field">
		<div class="control">
			<div class="select">
				<select bind:value={type}>
					<option value="">-- Select Type --</option>
					{#each itemTypes as type}
						<option value={type}>{type}</option>
					{/each}
				</select>
			</div>
		</div>
	</div>

	<div class="search-field">
		<input
			class="input search-input"
			type="text"
			bind:value={hash}
			placeholder="Enter hash"
			maxlength="64"
		/>
	</div>

	<div class="search-field">
		<button class="button is-info" on:click={fetchData} disabled={url === ''}>Search</button>
	</div>
</section>

<div class="url">{url}</div>

<div class="result">
	{#if error}
		<div class="error-message">{error}</div>
	{:else if data}
		<div class="data-box">
			<JSONTree {data} />
		</div>
	{/if}
</div>

<style>
	.search-bar {
		display: flex;
		align-items: flex-start;
		justify-content: flex-start;
		flex-direction: row;
		padding: 20px;
	}

	.search-field {
		display: flex;
		align-items: center;
		justify-content: flex-start;
	}

	.search-input {
		width: 650px;
		margin-right: 10px;
		padding: 8px;
		border: 1px solid #ccc;
		border-radius: 4px;
	}

	.result {
		margin-top: 20px;
	}

	.error-message {
		color: red;
	}

	.data-box {
		border: 1px solid #ccc;
		border-radius: 4px;
		padding: 10px;
		max-width: 100%;
		overflow: auto;
	}

	.url {
		margin-left: 25px;
		padding: 10px;
		font-size: 0.7rem;
	}
</style>
