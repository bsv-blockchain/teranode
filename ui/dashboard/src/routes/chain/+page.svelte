<script>
  import { onMount } from 'svelte'
  import { blocks } from '@stores/chainStore.js'
  import { goto } from '$app/navigation'
  import JSONTree from '@components/JSONTree.svelte'

  let treeData = {}

  onMount(() => {
    if ($blocks) {
      drawTree($blocks)
    }
  })

  $: {
    if ($blocks) {
      drawTree($blocks)
    }
  }

  function drawTree(blocks) {
    if (import.meta.env.SSR) return

    const width = 600
    const height = 400

    treeData = mapNamesToChildren(blocks)
    if (!treeData) return

    // Remove previous SVG content
    d3.select('#tree').select('svg').remove()

    const svg = window.d3
      .select('#tree')
      .append('svg')
      .attr('width', '100%')
      .attr('height', '100%')

    const root = d3.hierarchy(treeData)
    const treeLayout = d3.tree().size([height - 200, width])

    treeLayout(root)

    const g = svg.append('g').attr('transform', 'translate(50,0)')

    g.selectAll('.link')
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
        )
      })

    const node = g
      .selectAll('.node')
      .data(root.descendants())
      .enter()
      .append('g')
      .attr(
        'class',
        (d) => 'node' + (d.children ? ' node--internal' : ' node--leaf')
      )
      .attr('transform', (d) => 'translate(' + d.y + ',' + d.x + ')')

    node.each(function (d, i) {
      if (i === 0) {
        d3.select(this)
          .append('rect')
          .attr('width', 5)
          .attr('height', 5)
          .attr('fill', 'black')
      } else {
        node
          .append('circle')
          .attr('r', 10)
          .attr('fill', (d) => stringToColor(d.data.miner)) // add this line to derive color from the "miner" value
          .append('title')
          .text((d) => d.data.name + '\n' + d.data.miner)

        node
          .append('text')
          .attr('dy', 30)
          .attr('x', -15)
          .style('text-anchor', 'start')
          .text((d) => d.data.height)

        node.on('click', (event, d) => {
          handleClick(d.data)
        })
      }
    })
  }

  function handleClick(data) {
    goto(`/viewer/block/${data.name}/json`)
  }

  function mapNamesToChildren(arr) {
    const nodeMap = {}

    arr.forEach((item) => {
      // Create or get the current node
      if (!nodeMap[item.hash]) {
        nodeMap[item.hash] = {
          name: item.hash,
          height: item.height,
          miner: item.miner.split('/')[1],
          children: [],
        }
      }

      const currentNode = nodeMap[item.hash]

      // If there's no previous block hash, skip linking it to any parent
      if (!item.previousblockhash) {
        return
      }

      // Create or get the parent node
      if (!nodeMap[item.previousblockhash]) {
        nodeMap[item.previousblockhash] = {
          name: item.previousblockhash,
          height: item.height - 1, // Assuming the height of parent is always current height - 1
          miner: 'ROOT',
          children: [],
        }
      }

      const parentNode = nodeMap[item.previousblockhash]

      // Link the current node to its parent
      parentNode.children.push(currentNode)
    })

    // Find the actual root node (the node that isn't a child of any other node)
    const rootNode = Object.values(nodeMap).find(
      (node) => !arr.some((item) => item.hash === node.name)
    )

    return rootNode || {}
  }

  function stringToColor(str) {
    if (str === 'ROOT') return '#000000'

    const colors = ['green', 'red', 'blue', 'orange', 'magenta', '#33FFFF']
    let hash = 0

    for (let i = 0; i < str.length; i++) {
      hash += str.charCodeAt(i)
    }

    const index = hash % colors.length
    return colors[index]
  }
</script>

<section class="section">
  <div class="full">
    <div class="full" id="tree" />
  </div>

  <div>
    <JSONTree data={treeData} />
  </div>

  <pre>
    {#each $blocks as block (block.hash)}
      {'\n' + block.height + ': ' + block.hash}
    {/each}
  </pre>
</section>

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
    stroke-width: 2px;
    cursor: pointer;
  }

  :global(.node text) {
    font-size: 10px;
  }
</style>
