<script lang="ts">
  import { afterUpdate } from 'svelte'
  import { page } from '$app/stores'
  import SvelteMarkdown from 'svelte-markdown'

  import i18n from '$internal/i18n'

  $: t = $i18n.t

  const pageKey = 'page.notifications'

  $: slug = $page.params.slug

  let currentPost: any = null

  async function loadPost(slug: string) {
    currentPost = (await import(`../../../internal/assets/blog/${slug}.json`)).default
  }

  $: {
    if (slug) {
      loadPost(slug)
    }
  }

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
  <div class="post-details">
    <SvelteMarkdown source={currentPost?.body} />
  </div>
{/if}

<style>
  .post-details {
    box-sizing: var(--box-sizing);

    display: flex;
    flex-direction: column;
    gap: 6px;

    padding: 10px 20px 10px 30px;
    border-radius: 12px;
    border-left: 1px solid #151a20;
  }

  :global(.post-details table) {
    border: 1px solid white;
  }
  :global(.post-details table th, .msg-contaienr table td) {
    border: 1px solid white;
  }
</style>
