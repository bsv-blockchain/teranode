<script>
  import { onMount } from 'svelte'
  import { addSubscriber } from '@stores/bootstrapStore.js'
  import { selectedNode } from '@stores/nodeStore.js'
  import { blocks, isFirstMount } from '@stores/chainStore.js'
  import { goto } from '$app/navigation'
  import JSONTree from '@components/JSONTree.svelte'

  let treeData = {}

  onMount(() => {
    if ($isFirstMount) {
      isFirstMount.set(false)

      console.log('adding subscriber')
      addSubscriber(update)
    }

    drawTree($blocks)
    // return () => {
    //   console.log('removing subscriber')
    //   removeSubscriber(update)
    // }
  })

  function update(data) {
    if (data.type !== 'Block') return

    // Add the new block to the end of the list unless it already exists
    if ($blocks.find((block) => block.hash === data.hash)) return

    const b = [...$blocks]

    // Now fetch the block header
    const url = $selectedNode + '/header/' + data.hash + '/json'

    fetch(url)
      .then((res) => res.json())
      .then((json) => {
        console.log(`got block ${json.height}`)
        // Add the new block to the end of the list and remove the first block if the list is too long

        // Slice off all but the last 30 blocks
        if (b.length > 30) {
          b.splice(b.length - 30)
        }

        blocks.set([...b, json])

        drawTree($blocks)
      })
  }

  function drawTree(blocks) {
    const width = 600
    const height = 400

    treeData = mapNamesToChildren(blocks)
    if (!treeData) return

    // Remove previous SVG content
    d3.select('#tree').select('svg').remove()

    const svg = d3
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

    node
      .append('circle')
      .attr('r', 10)
      .append('title')
      .text((d) => d.data.name)
    node
      .append('text')
      .attr('dy', 30)
      .attr('x', -15)
      .style('text-anchor', (d) => (d.children ? 'start' : 'start'))
      // .text((d) => d.data.name.substring(0, 6));
      .text((d) => d.data.height)
    node.on('click', (event, d) => {
      handleClick(d.data)
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
    fill: #999;
    stroke: #555;
    stroke-width: 2px;
    cursor: pointer;
  }

  :global(.node text) {
    font-size: 10px;
  }
</style>
