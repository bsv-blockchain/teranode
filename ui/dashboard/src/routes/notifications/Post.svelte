<script lang="ts">
  export let title = ''
  export let summary = ''
  export let timestamp
  export let selected = false
</script>

<div class="post" class:selected on:click>
  <div class="left">
    <div class="title">{title}</div>
    <div class="summary">{summary}</div>
  </div>
  <div class="right">
    <div class="timestamp">{timestamp}</div>
  </div>
</div>

<style>
  .post {
    box-sizing: var(--box-sizing);
    width: 100%;

    display: flex;
    justify-content: space-between;

    padding: 10px;
    border-radius: 10px;

    cursor: pointer;
  }

  .post.selected {
    background: #151a20;
  }

  .left {
    flex: 1;

    display: flex;
    flex-direction: column;
  }

  .right {
    flex: 0;
    max-width: 200px;
  }
</style>
