<script>
  import { onMount } from 'svelte'
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

  function humanTime(time) {
    let diff = new Date().getTime() - new Date(time).getTime()
    diff = diff / 1000
    const days = Math.floor(diff / 86400)
    const hours = Math.floor((diff % 86400) / 3600)
    const minutes = Math.floor(((diff % 86400) % 3600) / 60)
    const seconds = Math.floor(((diff % 86400) % 3600) % 60)

    let difference = ''

    if (days > 0) {
      difference += days
      difference += ' day' + (days === 1 ? ', ' : 's, ')
      difference += hours
      difference += ' hour' + (hours === 1 ? ', ' : 's, ')
      difference += minutes
      difference += ' minute' + (minutes === 1 ? ' and ' : 's and ')
      difference += seconds
      difference += ' second' + (seconds === 1 ? '' : 's')
      return difference
    }

    if (hours > 0) {
      difference += hours
      difference += ' hour' + (hours === 1 ? ', ' : 's, ')
      difference += minutes
      difference += ' minute' + (minutes === 1 ? ' and ' : 's and ')
      difference += seconds
      difference += ' second' + (seconds === 1 ? '' : 's')
      return difference
    }

    if (minutes > 0) {
      difference += minutes
      difference += ' minute' + (minutes === 1 ? ' and ' : 's and ')
      difference += seconds
      difference += ' second' + (seconds === 1 ? '' : 's')
      return difference
    }

    if (seconds > 0) {
      difference += seconds
      difference += ' second' + (seconds === 1 ? '' : 's')
      return difference
    }

    return '0 seconds'
  }
</script>

Connected at {new Date(time).toISOString().replace('T', ' ')}
<br />
<span class="small">{age} ago</span>

<style>
  .small {
    font-size: 0.8rem;
  }
</style>
