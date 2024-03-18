<script lang="ts">
  import { tippy } from '$lib/stores/media'
  import { copyTextToClipboardVanilla } from '$lib/utils/clipboard'
  import ActionStatusIcon from '$internal/components/action-status-icon/index.svelte'

  import { reverseHashParam } from '$internal/utils/urls'

  import { Icon } from '$lib/components'
  import JSONTree from '$internal/components/json-tree/index.svelte'
  import Card from '$internal/components/card/index.svelte'
  import i18n from '$internal/i18n'
  import { getItemApiUrl, ItemType } from '$internal/api'

  const baseKey = 'page.viewer-utxo.details'

  $: t = $i18n.t

  export let data: any = {}

  function onReverseHash(hash) {
    reverseHashParam(hash)
  }
</script>

<Card title={t(`${baseKey}.title`)}>
  <div class="copy-link" slot="subtitle">
    <div class="hash">{data?.hash}</div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-hash-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-duplicate-line"
        action={copyTextToClipboardVanilla}
        actionData={data?.hash}
        size={15}
      />
    </div>
    <div class="icon" use:$tippy={{ content: t('tooltip.copy-url-to-clipboard') }}>
      <ActionStatusIcon
        icon="icon-bracket-line"
        action={copyTextToClipboardVanilla}
        actionData={getItemApiUrl(ItemType.subtree, data?.hash)}
        size={15}
      />
    </div>
    <div
      class="icon"
      on:click={() => onReverseHash(data?.hash)}
      use:$tippy={{ content: t('tooltip.reverse-hash') }}
    >
      <Icon name="icon-reeverse-line" size={15} />
    </div>
  </div>

  <div class="json">
    <div><JSONTree {data} /></div>
  </div>
</Card>

<style>
  .json {
    box-sizing: var(--box-sizing);
    margin-top: 12px;

    padding: 25px;
    border-radius: 10px;
    background: var(--app-bg-color);

    width: 100%;
    overflow-x: auto;
  }

  .copy-link {
    display: flex;
    word-break: break-all;
  }
  .icon {
    padding-top: 4px;
    padding-left: 8px;
    cursor: pointer;
  }
</style>
