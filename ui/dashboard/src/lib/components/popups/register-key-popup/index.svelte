<script lang="ts">
  import { createEventDispatcher } from 'svelte'
  import { form, field } from 'svelte-forms'
  import { required } from 'svelte-forms/validators'
  import { Button, Modal, Popup, TextInput } from '$lib/components'
  import Spacer from '../../../../lib/components/layout/spacer/index.svelte'
  import { link } from '../../../../lib/utils/format'
  import i18n from '../../../i18n'

  $: t = $i18n.t
  const tKey = 'comp.register-key-popup'

  const dispatch = createEventDispatcher()

  export let testId: string | undefined | null = null

  const apiKey = field('apiKey', '', [required()])
  const formInst = form(apiKey)

  let changedOrBlurred = false

  function onInputChange(e) {
    apiKey.set(e.detail.value)
    changedOrBlurred = true
  }

  function onInputMount(e) {
    e.detail.inputRef.focus()
  }

  function onBlur() {
    changedOrBlurred = true
    apiKey.validate()
  }

  function onClose() {
    dispatch('close')
  }
  function onRegisterClick() {
    dispatch('register', { apiKey: $apiKey.value })
  }

  apiKey.validate()
</script>

<Modal>
  <Popup maxW={480} title={t(`${tKey}.title`)} on:close={onClose} {testId}>
    <svelte:fragment slot="body">
      <Spacer h={20} />
      <TextInput
        name="api-key"
        size="large"
        label={t(`${tKey}.api-key-label`)}
        footnote={t(`${tKey}.api-key-hint`, {
          url: link('https://console.taal.com'),
        })}
        error={`${
          changedOrBlurred && $formInst.hasError('apiKey.required') ? t('validation.required') : ''
        }`}
        bind:value={$apiKey.value}
        required
        on:change={onInputChange}
        on:mount={onInputMount}
        on:blur={onBlur}
      />
      <Spacer h={80} />
    </svelte:fragment>
    <svelte:fragment slot="footer">
      <Button variant="ghost" size="medium" on:click={onClose}>
        {t(`${tKey}.cancel-label`)}
      </Button>
      <Button
        variant="secondary"
        size="medium"
        disabled={!$formInst.valid}
        on:click={onRegisterClick}
      >
        {t(`${tKey}.register-label`)}
      </Button>
    </svelte:fragment>
  </Popup>
</Modal>
