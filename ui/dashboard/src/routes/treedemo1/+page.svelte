<script>
	// import { blocks, error } from '@stores/nodeStore.js';
	import { blocks, error } from './testData.js';
	import { onMount } from 'svelte';

	function getUniqueValues(obj) {
		let values = [];
		for (let key in obj) {
			values = values.concat(obj[key]);
		}
		return [...new Set(values)];
	}

	function createHierarchy(arr) {
		const nodeMap = {};

		arr.forEach((item) => {
			// Create or get the current node
			if (!nodeMap[item.hash]) {
				nodeMap[item.hash] = {
					name: item.hash,
					height: item.height,
					children: []
				};
			}

			const currentNode = nodeMap[item.hash];

			// If there's no previous block hash, skip linking it to any parent
			if (!item.previousblockhash) {
				return;
			}

			// Create or get the parent node
			if (!nodeMap[item.previousblockhash]) {
				nodeMap[item.previousblockhash] = {
					name: item.previousblockhash,
					height: item.height - 1, // Assuming the height of parent is always current height - 1
					children: []
				};
			}

			const parentNode = nodeMap[item.previousblockhash];

			// Link the current node to its parent
			parentNode.children.push(currentNode);
		});

		// Find the actual root node (the node that isn't a child of any other node)
		const rootNode = Object.values(nodeMap).find(
			(node) => !arr.some((item) => item.hash === node.name)
		);

		return rootNode || {};
	}

	let unique = [];

	onMount(() => {
		const width = 600;
		const height = 400;

		const svg = d3.select('#tree').append('svg').attr('width', '100%').attr('height', '100vh');

		unique = getUniqueValues($blocks);

		const treeData = createHierarchy(unique);
		const root = d3.hierarchy(treeData);
		const treeLayout = d3.tree().size([height - 200, width]);

		treeLayout(root);

		const g = svg.append('g').attr('transform', 'translate(100,0)');

		const link = g
			.selectAll('.link')
			.data(root.descendants().slice(1))
			.enter()
			.append('path')
			.attr('class', 'link')
			.attr('d', (d) => {
				return (
					'M' +
					d.y +
					',' +
					d.x +
					'C' +
					(d.y + d.parent.y) / 2 +
					',' +
					d.x +
					' ' +
					(d.y + d.parent.y) / 2 +
					',' +
					d.parent.x +
					' ' +
					d.parent.y +
					',' +
					d.parent.x
				);
			});

		const node = g
			.selectAll('.node')
			.data(root.descendants())
			.enter()
			.append('g')
			.attr('class', (d) => 'node' + (d.children ? ' node--internal' : ' node--leaf'))
			.attr('transform', (d) => 'translate(' + d.y + ',' + d.x + ')');

		node
			.append('circle')
			.attr('r', 10)
			.append('title')
			.text((d) => d.data.name);
		node
			.append('text')
			.attr('dy', 30)
			.attr('x', -15)
			.style('text-anchor', (d) => (d.children ? 'start' : 'start'))
			// .text((d) => d.data.name.substring(0, 6));
			.text((d) => d.data.height);
	});
</script>

<div class="full">
	{#if $error}
		<p>{$error}</p>
	{:else}
		<section class="section">
			<div class="field">Last 10 blocks from demo data</div>
			<div class="full" id="tree" />
			<pre>{JSON.stringify(unique, null, 2)}</pre>
		</section>
	{/if}
</div>

<style>
	.full {
		width: 100%;
		height: 100vh;
	}

	:global(.link) {
		fill: none;
		stroke: #555;
		stroke-opacity: 0.4;
		stroke-width: 2;
	}
	:global(.node circle) {
		fill: #999;
		stroke: #555;
		stroke-width: 2px;
	}
	:global(.node text) {
		font-size: 10px;
	}
</style>
