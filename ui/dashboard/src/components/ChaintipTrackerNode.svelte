<script>
  export let node

  function shortHash(hash) {
    if (!hash) return ''

    return hash.substring(0, 8) + '...' + hash.substring(hash.length - 8)
  }
</script>

<tr>
  <!-- <td>{node.source || 'anonymous'}</td> -->
  <!-- <td>{node.name}</td> -->
  <!-- <td>{node.blobServerHTTPAddress}</td> -->
  <td class="right">{node.header.height}</td>
  <td title={node.header.hash}>{shortHash(node.header.hash)}</td>
  <td title={node.header.previousblockhash}
    >{shortHash(node.header.previousblockhash)}</td
  >
</tr>

<style>
  td {
    border: 1px solid #e5e5e5; /* Add a light border to table cells */
    padding: 8px 12px; /* Add some padding to table cells */
  }

  .right {
    text-align: right;
  }

  @media (max-width: 600px) {
    tr {
      display: grid;
      grid-template-columns: 1fr 1fr;
      grid-template-rows: repeat(2, auto);
    }

    td {
      grid-column: span 1;
    }
  }
</style>
