<script>
	let id = '';
	let type = '';
	let data = null;
	let addr = 'https://miner1.ubsv.dev:8090';
	let error = null;

	const itemTypes = ['tx', 'subtree', 'header', 'block', 'utxo'];

	async function fetchData() {
		try {
			const url = `${addr}/${type}/${id}/json`;
			console.log(url);
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
	<h1 class="title">Enter ID and Select Item Type to Fetch Data</h1>
	<div class="field is-horizontal">
		<div class="field-label is-normal">
			<label class="label">Item Type</label>
		</div>
		<div class="field-body">
			<div class="field">
				<div class="control">
					<div class="select">
						<select bind:value={type}>
							<option value="">-- Select Item Type --</option>
							{#each itemTypes as type}
								<option value={type}>{type}</option>
							{/each}
						</select>
					</div>
				</div>
			</div>
			<div class="field">
				<div class="control">
					<input class="input" type="text" bind:value={addr} placeholder="Enter HTTP address" />
				</div>
			</div>
			<div class="field">
				<div class="control">
					<input class="input hex-input" type="text" bind:value={id} placeholder="Enter ID" />
				</div>
			</div>
			<div class="field">
				<div class="control">
					<button class="button is-primary" on:click={fetchData}>Fetch</button>
				</div>
			</div>
		</div>
	</div>

	{#if error}
		<div class="notification is-danger">{error}</div>
	{:else if data}
		<div class="box">
			<pre>
        {JSON.stringify(data, null, 2)}
      </pre>
			<!-- Display the fetched data here. Adjust based on your data structure. -->
		</div>
	{/if}
</section>

<style>
	.hex-input {
		width: 600px; /* Adjust this value based on the font and size you're using */
		max-width: 100%; /* Ensure it doesn't overflow on smaller screens */
	}
</style>
