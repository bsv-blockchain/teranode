<script lang="ts">
  import { Icon } from '$lib/components'
  import type { SidebarSection, SidebarSectionItem } from './types'

  export let section: SidebarSection
  export let renderTitle: boolean = true
  export let onLink: (item: SidebarSectionItem, type: string) => void
</script>

{#if section}
  <div class="section" class:blue={section.variant === 'blue'}>
    {#if renderTitle && section.title}
      <div class="section-title">{section.title}</div>
    {/if}
    <div class="section-items">
      {#each section.items as item, j (j)}
        <!-- svelte-ignore a11y-click-events-have-key-events -->
        <div class="section-item" on:click={onLink ? (e) => onLink(item, section.type) : null}>
          {#if item.icon}
            <Icon name={item.icon} size={32} />
          {/if}
          <span>{item.label}</span>
        </div>
      {/each}
    </div>
  </div>
{/if}

<style>
  .section {
    margin: 24px 16px;
  }

  .section-title {
    /* Heading 7 */
    font-family: 'Inter';
    font-weight: 600;
    font-size: 12px;
    line-height: 18px;
    letter-spacing: 0.01em;

    /* Gray/600 */
    color: #8f8d94;

    padding: 0 16px 24px 16px;
  }

  .section-items {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0px;

    /* Heading 6 */

    font-family: 'Inter';
    font-style: normal;
    font-weight: 600;
    font-size: 14px;
    line-height: 20px;
    letter-spacing: 0.01em;

    /* Gray/800 */

    color: #282933;
  }
  .section.blue .section-items {
    color: #232d7c;
  }

  .section-item {
    display: flex;
    align-items: center;
    /* width: calc(100% - 32px); */
    gap: 10px;
    flex-wrap: nowrap;
    height: 48px;
    padding: 0 16px;

    cursor: pointer;
  }
</style>
