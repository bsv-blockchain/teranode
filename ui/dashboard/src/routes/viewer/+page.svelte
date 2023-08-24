<script>
	let hash = '';
	let type = '';
	let data = null;
	let addr = 'https://miner1.ubsv.dev:8090';
	let error = null;

	$: url = addr + '/' + type + '/' + hash + '/json';

	const itemTypes = ['tx', 'subtree', 'header', 'block', 'utxo'];

	async function fetchData() {
		try {
			const response = await fetch(url);
			if (!response.ok) {
				throw new Error(`HTTP error! Status: ${response.status}`);
			}
			data = await response.json();
		} catch (err) {
			error = err.message;
		}
	}
</script>

<section class="section">
	<div class="field">
		<p>Select the type and enter the hash to fetch data.</p>
	</div>

	<div class="field">
		<label class="label"
			>HTTP Address
			<div class="control">
				<input class="input" type="text" bind:value={addr} placeholder="Enter HTTP address" />
			</div>
		</label>
	</div>

	<div class="field">
		<label class="label"
			>Type
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
		</label>
	</div>

	<div class="field">
		<label class="label"
			>Hash
			<div class="control">
				<input class="input hex-input" type="text" bind:value={hash} placeholder="Enter ID" />
			</div>
		</label>
	</div>

	<div class="field">
		<div class="control">
			{url}
		</div>
	</div>

	<div class="field">
		<div class="control">
			<button class="button is-primary" on:click={fetchData}>Fetch</button>
		</div>
	</div>

	{#if error}
		<div class="notification is-danger">{error}</div>
	{:else if data}
		<div class="box">
			<pre>
        {JSON.stringify(data, null, 2)}
      </pre>
		</div>
	{/if}
</section>

<style>
	.hex-input {
		width: 650px;
		max-width: 100%;
	}
</style>
