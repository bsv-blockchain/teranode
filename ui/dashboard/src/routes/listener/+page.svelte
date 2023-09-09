<script>
  import { onMount } from 'svelte'
  import { addSubscriber, removeSubscriber } from '@stores/websocketStore.js'
  import { notifications } from '@stores/listenerStore.js'

  onMount(() => {
    console.log('adding subscriber')
    addSubscriber(update)
    return () => {
      console.log('removing subscriber')
      removeSubscriber(update)
    }
  })

  function update(json) {
    console.log(`received notification: ${JSON.stringify(json)}`)
    // Add the new notification to the end of the list
    notifications.set([...$notifications, json])
  }
</script>

<pre>
  {#each $notifications as notification}
    {'\n' + JSON.stringify(notification)}
  {/each}
</pre>
