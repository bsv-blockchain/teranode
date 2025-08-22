<script lang="ts">
  export let text: string = ''
  export let onClick: () => void = () => {}
  export let className: string = ''
</script>

<span 
  class="clickable-span {className}" 
  on:click={onClick}
  on:keydown={(e) => e.key === 'Enter' && onClick()}
  role="button"
  tabindex="0"
>
  {text}
</span>

<style>
  .clickable-span {
    cursor: pointer;
    color: var(--link-color, #0066cc);
    text-decoration: none;
    transition: all 0.2s ease;
    display: inline-block;
    padding: 2px 4px;
    border-radius: 4px;
  }

  .clickable-span:hover {
    background-color: var(--hover-bg, rgba(0, 102, 204, 0.1));
    text-decoration: underline;
  }

  .clickable-span:focus {
    outline: 2px solid var(--focus-color, #0066cc);
    outline-offset: 2px;
  }

  .clickable-span:active {
    transform: scale(0.98);
  }
</style>