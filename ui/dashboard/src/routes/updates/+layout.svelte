<script lang="ts">
  import type { LayoutData } from './$types'
  import { page } from '$app/stores'
  import { goto } from '$app/navigation'
  import { fade, fly } from 'svelte/transition'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import Post from '$internal/components/post/index.svelte'
  import i18n from '$internal/i18n'

  $: slug = $page.params.slug

  $: t = $i18n.t

  export let data: LayoutData
  const flyContent = true

  let sortedPosts: any[] = []
  $: {
    sortedPosts = data.posts.sort((a: any, b: any) => b.timestamp - a.timestamp)
  }

  const pageKey = 'page.notifications'

  function onPostSelect(slug: string) {
    goto(`/updates/${slug}`)
  }
</script>

<PageWithMenu showTools={true} showWarning={true}>
  <div class="tools-container">
    <div class="tools">
      <div class="title">{t(`${pageKey}.title`)}</div>
    </div>
  </div>
  <div class="layout">
    <div class="posts">
      {#each sortedPosts as post (post.slug)}
        <Post
          title={post.title}
          summary={post.summary}
          timestamp={post.timestamp}
          selected={slug === post.slug}
          on:click={() => onPostSelect(post.slug)}
        />
      {/each}
    </div>
    {#if slug}
      <div
        class="slug"
        in:fly={flyContent ? { x: 200, opacity: 0, duration: 300 } : {}}
        out:fade={{ delay: 100 }}
      >
        <slot />
      </div>
    {/if}
  </div>
</PageWithMenu>

<style>
  .tools-container {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

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

  .layout {
    font-family: var(--font-family);
    box-sizing: var(--box-sizing);

    width: 100%;
    margin-top: 20px;

    display: flex;
    flex-wrap: wrap;
    gap: 20px;
  }

  .posts {
    flex: 1 1 auto;
    min-width: 250px;

    display: flex;
    flex-direction: column;
    gap: 5px;
  }

  .slug {
    flex: 1 1 auto;
    min-width: 55%;
  }
</style>
