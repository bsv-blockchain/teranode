<script lang="ts">
  import { humanTime } from '$internal/utils/format'

  export let title = ''
  export let summary = ''
  export let timestamp
  export let selected = false
</script>

<div class="post" class:selected on:click>
  <div class="left">
    <div class="title">
      <div class="circle" />
      {title}
    </div>
    <div class="summary">{summary}</div>
  </div>
  <div class="right">
    <div class="timestamp">{`${humanTime(parseInt(timestamp))} ago`}</div>
  </div>
</div>

<style>
  .post {
    box-sizing: var(--box-sizing);
    width: 100%;

    display: flex;
    justify-content: space-between;

    padding: 10px 20px;
    border-radius: 10px;

    cursor: pointer;

    font-family: var(--font-family);
    font-weight: 700;
    font-size: 15px;
    font-style: normal;
    outline: none;
  }

  .post.selected,
  .post:hover {
    background: #151a20;
  }

  .post.selected {
    cursor: auto;
    pointer-events: none;
  }

  .left {
    flex: 1;

    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .title {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .title .circle {
    width: 8px;
    height: 8px;
    border-radius: 4px;
    background: #1778ff;
  }
  .summary {
    margin-left: 22px;

    font-weight: 400;
    font-size: 13px;
    opacity: 0.66;
  }

  .right {
    flex: 0.2;
    max-width: 200px;
    text-align: right;

    font-weight: 400;
    font-size: 13px;
    opacity: 0.66;
  }
</style>
