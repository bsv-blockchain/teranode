<script>
	import { blocks, error } from '@stores/nodeStore.js';
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
		let rootNode = null;

		// First pass: Create nodes and identify the root node
		arr.forEach((item) => {
			// Create or get the current node
			if (!nodeMap[item.hash]) {
				nodeMap[item.hash] = { name: item.hash, children: [] };
			}
			const currentNode = nodeMap[item.hash];

			// Create or get the parent node
			if (!nodeMap[item.previousblockhash]) {
				nodeMap[item.previousblockhash] = { name: item.previousblockhash, children: [] };
			}
			const parentNode = nodeMap[item.previousblockhash];

			// Link the current node to its parent
			parentNode.children.push(currentNode);

			// If the parent node doesn't have a parent itself, it's the root
			if (!arr.some((i) => i.hash === item.previousblockhash)) {
				rootNode = parentNode;
			}
		});

		return {
			name: 'Root',
			children: rootNode ? [rootNode] : []
		};
	}

	let unique = [];

	onMount(() => {
		const width = 600;
		const height = 400;

		const svg = d3.select('#tree').append('svg').attr('width', '100%').attr('height', '100vh');

		// const treeData = {
		// 	name: 'Root',
		// 	children: [
		// 		{ name: 'Child 1' },
		// 		{
		// 			name: 'Child 2',
		// 			children: [{ name: 'Grandchild 1' }, { name: 'Grandchild 2' }]
		// 		}
		// 	]
		// };
		unique = getUniqueValues($blocks);

		const treeData = createHierarchy(unique);
		debugger;

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
			.attr('x', (d) => (d.children ? -15 : 15))
			.style('text-anchor', (d) => (d.children ? 'start' : 'start'))
			// Add tooltip
			.text((d) => d.data.name.substring(0, 6));
	});
</script>

<div class="full">
	{#if $error}
		<p>{$error}</p>
	{:else}
		<div class="full" id="tree" />

		<pre>{JSON.stringify(unique, null, 2)}</pre>
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
