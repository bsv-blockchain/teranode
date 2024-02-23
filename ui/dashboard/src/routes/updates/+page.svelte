<script lang="ts">
  import { afterUpdate } from 'svelte'

  import SvelteMarkdown from 'svelte-markdown'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  const pageKey = 'page.updates'

  const source = `
  # This is a header

This is a paragraph.

* This is a list
* With two items
  1. And a sublist
  2. That is ordered
    * With another
    * Sublist inside

👋 hello

| And this is | A table |
|-------------|---------|
| With two    | columns |`

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

<PageWithMenu>
  <div class="tools-container">
    <div class="tools">
      <div class="title">{t(`${pageKey}.title`)}</div>
    </div>
  </div>

  <div class="container">
    <div class="column">
      <div class="msg-contaienr">
        <SvelteMarkdown {source} />
      </div>
    </div>
  </div>
</PageWithMenu>

<style>
  .tools-container {
    flex: 1;

    width: 100%;
    min-height: 50px;
    padding: 24px;

    border-radius: 12px;
    background: linear-gradient(0deg, rgba(255, 255, 255, 0.04) 0%, rgba(255, 255, 255, 0.04) 100%),
      #0a1018;
  }

  .tools {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    justify-content: space-between;

    margin-top: -8px;
  }
  .tools .title {
    color: rgba(255, 255, 255, 0.88);

    font-family: var(--font-family);
    font-size: 22px;
    font-style: normal;
    font-weight: 700;
    line-height: 28px;
    letter-spacing: 0.44px;

    margin-top: 8px;
  }

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
