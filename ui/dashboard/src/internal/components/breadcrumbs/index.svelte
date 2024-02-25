<script lang="ts">
  import { beforeUpdate } from 'svelte'
  import { page } from '$app/stores'
  import { getDetailsUrl } from '$internal/utils/urls'
  import i18n from '../../i18n'

  export let showOnRoot = false

  const baseKey = 'comp.breadcrumbs.page'

  let ready = false
  beforeUpdate(() => {
    ready = true
  })

  let data: any[] = []

  $: {
    const tmp: any[] = []
    const pathname = ready ? $page.url.pathname : ''

    let paths = pathname.split('/').filter((path) => path.length > 0)

    if (paths.length === 0) {
      tmp.push({
        label: $i18n.t(`${baseKey}.home.title`),
        path: `/`,
        selected: true,
      })
    } else {
      // viewer subpages
      switch (paths[0]) {
        case 'viewer':
          tmp.push({
            label: $i18n.t(`${baseKey}.${paths[0]}.title`),
            path: `/viewer/`,
            selected: paths.length === 1,
          })
          const type = paths[1]
          const hash = ready ? $page.url.searchParams.get('hash') ?? '' : ''
          if (paths.length === 2 && hash) {
            tmp.push({
              label: $i18n.t(`${baseKey}.viewer.page.${type}.title`),
              path: getDetailsUrl(type, hash),
              selected: paths.length === 2,
            })
          }
          break
        // case 'updates':
        //   tmp.push({
        //     label: 'updates',
        //     path: '/updates/',
        //     selected: paths.length === 1,
        //   })
        //   if (paths.length === 2) {
        //     tmp.push({
        //       label: paths[1],
        //       path: `/updates/${paths[1]}`,
        //       selected: paths.length === 2,
        //     })
        //   }
        //   break
      }
    }

    data = [...tmp]
  }
</script>

{#if showOnRoot || data.length > 1}
  <div class="tui-breadcrumbs">
    {#each data as crumb, i}
      <a href={crumb.path} class="crumb" class:selected={i === data.length - 1}>{crumb.label}</a>
      {#if i < data.length - 1}
        /
      {/if}
    {/each}
  </div>
{/if}

<style>
  .tui-breadcrumbs {
    display: flex;
    gap: 5px;

    font-family: Satoshi;
    font-size: 13px;
    font-style: normal;
    font-weight: 400;
    line-height: 18px;
    letter-spacing: 0.26px;
  }

  .tui-breadcrumbs .crumb {
    color: rgba(255, 255, 255, 0.66);
  }
  .tui-breadcrumbs .crumb:hover,
  .tui-breadcrumbs .crumb.selected {
    color: rgba(255, 255, 255, 0.88);
    font-weight: 700;
  }
</style>
