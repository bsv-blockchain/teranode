<script lang="ts">
  import { afterUpdate } from 'svelte'
  import SvelteMarkdown from 'svelte-markdown'

  import i18n from '$internal/i18n'

  $: t = $i18n.t

  const pageKey = 'page.notifications'

  export let slug = 'welcome'

  let currentPost: any = null

  async function loadPost(slug: string) {
    currentPost = (await import(`../../../internal/assets/blog/${slug}.json`)).default
  }

  $: {
    if (slug) {
      loadPost(slug)
    }
  }

  $: console.log('currentPost = ', currentPost)

  //   const source = `
  //     # This is a header

  //   This is a paragraph.

  //   * This is a list
  //   * With two items
  //     1. And a sublist
  //     2. That is ordered
  //       * With another
  //       * Sublist inside

  //   👋 hello

  //   | And this is | A table |
  //   |-------------|---------|
  //   | With two    | columns |`

  function scrollToTop() {
    if (!import.meta.env.SSR && window && window.scrollTo) {
      // Check if the user is near the top of the scroll (e.g., within the top 100 pixels)
      const scrollThreshold = 100 // Adjust this threshold as needed

      if (window.scrollY <= scrollThreshold) {
        window.scrollTo({ top: 0, behavior: 'smooth' })
      }
    }
  }

  afterUpdate(() => {
    scrollToTop()
  })
</script>

{#if currentPost}
  <div class="container">
    <div class="column">
      <div class="msg-contaienr">
        <SvelteMarkdown source={currentPost?.body} />
      </div>
    </div>
  </div>
{/if}

<style>
  .container {
    box-sizing: var(--box-sizing);
    margin-top: 20px;

    display: flex;
    align-items: flex-start;
    gap: 10px;

    width: 100%;
    max-width: 100%;
    overflow-x: auto;
  }

  .msg-contaienr {
    flex: 1;

    box-sizing: var(--box-sizing);
    display: flex;
    flex-direction: column;
    gap: 6px;

    background: #151a20;
    padding: 20px;
    border-radius: 12px;
  }

  :global(.msg-contaienr table) {
    border: 1px solid white;
  }
  :global(.msg-contaienr table th, .msg-contaienr table td) {
    border: 1px solid white;
  }

  .column {
    flex: 1;
    min-width: 200px;
  }

  * {
    box-sizing: var(--box-sizing);
  }
</style>
