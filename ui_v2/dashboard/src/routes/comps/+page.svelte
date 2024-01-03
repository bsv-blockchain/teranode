<script lang="ts">
  import PageBasic from '$internal/components/page/template/basic/index.svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { Button, Dropdown, TextInput } from '$lib/components'
  import i18n from '$internal/i18n'

  $: t = $i18n.t

  const withMenu = true
</script>

{#if withMenu}
  <PageWithMenu>
    <div class="comps">
      <section class="comp-secion">
        <div class="title">Buttons</div>
        <Button size="xlarge" round={true} stretch={true}>Hello</Button>
        <Button size="xlarge" ico={true} icon="icon-chevron-left-line" />
        <!-- <Button size="xlarge">Hello</Button>
        <Button size="xlarge" disabled={true}>Hello</Button>
        <Button size="large" round={true} stretch={true}>Hello</Button> -->
        <Button size="large" ico={true} icon="icon-chevron-left-line" />
        <!-- <Button size="large">Hello</Button>
        <Button size="large" disabled={true}>Hello</Button>
        <Button size="medium" round={true} stretch={true}>Hello</Button> -->
        <Button size="medium" ico={true} icon="icon-chevron-left-line" />
        <!-- <Button size="medium">Hello</Button>
        <Button size="medium" disabled={true}>Hello</Button>
        <Button size="small" round={true} stretch={true}>Hello</Button> -->
        <Button size="small" ico={true} icon="icon-chevron-left-line" />
        <Button size="small">Hello</Button>
        <Button size="small" disabled={true}>Hello</Button>
        <Button variant="secondary" size="large" round={true}>Hello</Button>
        <Button variant="secondary" size="large" stretch={true}>Hello</Button>
        <Button variant="secondary" size="large" disabled={true}>Hello</Button>

        <Button size="small" variant="tertiary">Hello</Button>
        <Button variant="tertiary" size="large" round={true}>Hello</Button>
        <Button size="small" disabled={true} variant="destructive">Hello</Button>
        <Button variant="destructive" size="large" stretch={true}>Hello</Button>
      </section>
      <section class="comp-secion">
        <div class="title">Dropdowns</div>
        <Dropdown
          size="large"
          label="Label"
          items={[
            { value: 0, label: 'Zero' },
            { value: 1, label: 'One' },
            { value: 2, label: 'Two' },
          ]}
        />
        <Dropdown
          size="medium"
          label="Label"
          items={[
            { value: 0, label: 'Zero' },
            { value: 1, label: 'One' },
            { value: 2, label: 'Two' },
          ]}
          expandUp={true}
          stretch={true}
        />
        <Dropdown
          size="small"
          label="Label"
          items={[
            { value: 0, label: 'Zero' },
            { value: 1, label: 'One' },
            { value: 2, label: 'Two' },
          ]}
          value={0}
          expandUp={true}
        />
        <Dropdown
          size="small"
          items={[
            { value: 0, label: 'Zero' },
            { value: 1, label: 'One' },
            { value: 2, label: 'Two' },
          ]}
          expandUp={true}
        />
      </section>
      <section class="comp-secion wide">
        <div class="title">TextInputs</div>
        <TextInput name="one" value="1231" />
        <TextInput name="one" placeholder="1231" value="" />
        <TextInput name="one" value="1231" width={150} focusWidth={240} />
        <TextInput name="one" value="1231" width={150} focusWidth={240} icon="icon-search-line" />
      </section>
    </div>
  </PageWithMenu>
{:else}
  <PageBasic>
    <section>
      {t('page.home.title')}
    </section>
  </PageBasic>
{/if}

<style>
  .comps {
    width: 100%;
    display: flex;
    gap: 40px;
  }

  .comp-secion {
    width: 140px;
    padding: 20px;
    /* border: 1px solid rgba(255, 255, 255, 0.5); */
    border-radius: 10px;
    background-color: var(--comp-bg-color);

    display: flex;
    flex-direction: column;
    gap: 2px;
  }
  .comp-secion.wide {
    width: 240px;
  }

  .title {
    padding-bottom: 20px;
  }
</style>
