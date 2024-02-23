<script lang="ts">
  import type { LayoutData } from './$types'

  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  export let data: LayoutData

  $: console.log('data =', data)

  const pageKey = 'page.notifications'
</script>

<PageWithMenu showTools={false} showWarning={true}>
  <div class="tools-container">
    <div class="tools">
      <div class="title">{t(`${pageKey}.title`)}</div>
    </div>
  </div>
  <div class="layout">
    <div class="posts">fdgdfgdfg</div>
    <div class="slug">
      <slot />
    </div>
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

    display: flex;
    flex-wrap: wrap;
    width: 100%;
    background: red;
  }

  .posts {
    flex: 1 1 auto;
    min-width: 250px;
  }

  .slug {
    flex: 1 1 auto;
    min-width: 400px;
  }
</style>
