<script>
  export let data = {}
</script>

<div class="card-content">
  <div class="content">
    <div class="column">
      {data.hash}
      <div class="row">{data.height}</div>
    </div>
    <div class="data-grid">
      <div class="data-cell">
        <div class="label">Previous Block:</div>
        <div class="data-content">{data.previousblockhash}</div>
      </div>
      <div class="data-cell">
        <div class="label">Version:</div>
        <div class="data-content">{data.version}</div>
      </div>
      <div class="data-cell">
        <div class="label">Merkle Root:</div>
        <div class="data-content">{data.merkleroot}</div>
      </div>
      <div class="data-cell">
        <div class="label">Time:</div>
        <div class="data-content">{data.time}</div>
      </div>
      <div class="data-cell">
        <div class="label">Bits:</div>
        <div class="data-content">{data.bits}</div>
      </div>
      <div class="data-cell">
        <div class="label">Nonce:</div>
        <div class="data-content">{data.nonce}</div>
      </div>
    </div>
  </div>
</div>

<style>
  .data-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
  }

  .data-cell {
    display: inline-block;
    position: relative;
    padding: 1rem;
    border: 1px solid #e4e4e4;
    border-radius: 4px;
  }

  .label {
    position: absolute;
    top: 5px;
    left: 5px;
    font-size: 0.7rem;
    color: #666;
  }

  .data-content {
    font-size: 1rem;
    color: #333;
  }
</style>
