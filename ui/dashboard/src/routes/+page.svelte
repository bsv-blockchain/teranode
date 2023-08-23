<script>
	import ConnectedNodes from '@components/ConnectedNodes.svelte';
	import ChaintipTracker from '@components/ChaintipTracker.svelte';
	import { onMount, onDestroy } from 'svelte';

	let nodes = [];
	let loading = true;
	let error = null;
	let intervalId;

	async function fetchData() {
		try {
			const response = await fetch('https://bootstrap.ubsv.dev:8099/nodes');

			if (!response.ok) {
				throw new Error(`HTTP error! Status: ${response.status}`);
			}

			nodes = await response.json();
			error = null;
			loading = false;
		} catch (e) {
			error = e.message;
		}
	}

	onMount(() => {
		fetchData(); // Fetch data immediately when component is mounted
		intervalId = setInterval(fetchData, 2000); // Re-fetch every 2s seconds

		return () => clearInterval(intervalId); // Cleanup when component is destroyed
	});
</script>

<svelte:head>
	<!-- Include Bulma CSS for the entire app -->
	<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bulma@0.9.3/css/bulma.min.css" />

	<style>
		body {
			font-family: 'Arial', sans-serif;
			background-color: #f4f4f4;
			margin: 0;
			padding: 0;
			padding-top: 52px; /* Adjust for the height of the fixed navbar */
		}

		/* Custom darker blue color */
		.navbar.is-primary {
			background-color: #004466; /* Change this value to your preferred shade of dark blue */
		}

		/* Add more global styles as needed */
	</style>
</svelte:head>

<nav class="navbar is-primary is-fixed-top">
	<div class="navbar-brand">
		<!-- svelte-ignore a11y-invalid-attribute -->
		<a class="navbar-item" href="#"><h1>Teranode Dashboard</h1></a>
		<!-- svelte-ignore a11y-invalid-attribute -->
		<a
			role="button"
			class="navbar-burger"
			href="#"
			aria-label="menu"
			aria-expanded="false"
			data-target="navbarBasicExample"
		>
			<span aria-hidden="true" />
			<span aria-hidden="true" />
			<span aria-hidden="true" />
		</a>
	</div>

	<div id="navbarBasicExample" class="navbar-menu">
		<div class="navbar-start">
			<!-- Add navbar items here -->
			<!-- svelte-ignore a11y-invalid-attribute -->
			<a class="navbar-item" href="#"> Home </a>
			<!-- svelte-ignore a11y-invalid-attribute -->
			<a class="navbar-item" href="#"> Documentation </a>
		</div>

		<div class="navbar-end">
			<div class="navbar-item">
				<div class="buttons">
					<!-- Add buttons or other items here -->
					<!-- svelte-ignore a11y-invalid-attribute -->
					<a class="button is-light" href="#"> Log in </a>
				</div>
			</div>
		</div>
	</div>
</nav>

<div>
	<ConnectedNodes {nodes} {loading} {error} />
	<ChaintipTracker {nodes} {loading} />
</div>
