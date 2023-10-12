<script>
  import { onMount } from 'svelte'
  import { humanTime } from '@utils/humanTime.js'

  export let time
  let age = ''

  onMount(() => {
    age = humanTime(time)

    const interval = setInterval(() => {
      if (time) {
        age = humanTime(time)
      }
    }, 1000)

    return () => clearInterval(interval)
  })
</script>

Connected at {new Date(time).toISOString().replace('T', ' ')}
<br />
<span class="small">{age} ago</span>

<style>
  .small {
    font-size: 0.8rem;
  }
</style>
