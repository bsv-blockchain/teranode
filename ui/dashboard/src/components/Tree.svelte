<script>
	import { onMount } from 'svelte';

	let svg;

	onMount(() => {
		const width = 600;
		const height = 400;

		svg = d3.select('#tree').append('svg').attr('width', width).attr('height', height);

		const treeData = {
			name: 'Root',
			children: [
				{ name: 'Child 1' },
				{
					name: 'Child 2',
					children: [{ name: 'Grandchild 1' }, { name: 'Grandchild 2' }]
				}
			]
		};

		const root = d3.hierarchy(treeData);
		const treeLayout = d3.tree().size([height, width - 200]);

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

		node.append('circle').attr('r', 10);
		node
			.append('text')
			.attr('dy', 3)
			.attr('x', (d) => (d.children ? -15 : 15))
			.style('text-anchor', (d) => (d.children ? 'end' : 'start'))
			.text((d) => d.data.name);
	});
</script>

<div id="tree" />

<style>
	.link {
		fill: none;
		stroke: #555;
		stroke-opacity: 0.4;
		stroke-width: 2;
	}
	.node circle {
		fill: #999;
		stroke: #555;
		stroke-width: 2px;
	}
	.node text {
		font-size: 12px;
	}
</style>
