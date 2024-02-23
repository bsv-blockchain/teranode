<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import i18n from '$internal/i18n'

  const dispatch = createEventDispatcher()

  export let title = ''
  export let summary = ''
  export let timestamp
  export let selected = false

  $: timeStringParts = new Intl.DateTimeFormat($i18n.language, {
    timeStyle: 'medium',
    dateStyle: 'short',
  })
    .format(new Date(parseInt(timestamp)))
    .split(', ')

  function onKeyDown(e) {
    if (!e) e = window.event
    const keyCode = e.code || e.key
    if (keyCode === 'Enter') {
      dispatch('click', e)
      return false
    }
    dispatch('keydown', e)
  }
</script>

<div class="post" class:selected tabindex={0} on:keydown={onKeyDown} on:click role="button">
  <div class="left">
    <div class="title">
      <div class="circle" />
      {title}
    </div>
    <div class="summary">{summary}</div>
  </div>
  <div class="right">
    <div class="timestamp">
      {timeStringParts[0]}<br />
      <span class="time">{timeStringParts[1]}</span>
    </div>
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
    border: 1px solid transparent;

    cursor: pointer;

    font-family: var(--font-family);
    font-weight: 700;
    font-size: 15px;
    font-style: normal;
    outline: none;

    background-color: transparent;
    transition: background-color var(--easing-duration, 0.2s) var(--easing-function, ease-in-out);
  }

  .post.selected,
  .post:hover {
    background-color: #151a20;
  }

  .post:focus-visible {
    border: 1px solid rgba(255, 255, 255, 0.66);
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
    margin-left: 20px;

    font-weight: 400;
    font-size: 13px;
    opacity: 0.66;
  }

  .right {
    flex: 0.2;
    max-width: 80px;
    text-align: right;
    word-wrap: break-word;

    font-weight: 400;
    font-size: 13px;
    opacity: 0.66;
  }
  .time {
    font-size: 11px;
  }
</style>
