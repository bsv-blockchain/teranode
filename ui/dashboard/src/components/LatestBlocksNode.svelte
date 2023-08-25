<script>
	export let data = {};
</script>

<section class="section">
	<div class="container">
		<h1 class="title">Block Data</h1>
		<div class="box">
			<div class="field">
				<label class="label"
					>Hash
					<div class="control">
						<input class="input" type="text" value={data.hash} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Version
					<div class="control">
						<input class="input" type="number" value={data.version} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Previous Block Hash
					<div class="control">
						<input class="input" type="text" value={data.previousblockhash} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Merkle Root
					<div class="control">
						<input class="input" type="text" value={data.merkleroot} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Time
					<div class="control">
						<input class="input" type="number" value={data.time} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Bits
					<div class="control">
						<input class="input" type="text" value={data.bits} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Nonce
					<div class="control">
						<input class="input" type="number" value={data.nonce} readonly />
					</div>
				</label>
			</div>

			<div class="field">
				<label class="label"
					>Height
					<div class="control">
						<input class="input" type="number" value={data.height} readonly />
					</div>
				</label>
			</div>
		</div>
	</div>
</section>
