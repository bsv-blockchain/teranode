<script lang="ts">
  import { goto } from '$app/navigation';
  import { logout } from '$internal/stores/authStore';
  
  export let buttonText = 'Logout';
  export let redirectTo = '/login';
  export let buttonClass = '';
  
  let loading = false;
  
  async function handleLogout() {
    loading = true;
    
    try {
      await logout();
      goto(redirectTo);
    } catch (error) {
      console.error('Logout error:', error);
      // Even if the server-side logout fails, we'll still redirect to the login page
      goto(redirectTo);
    } finally {
      loading = false;
    }
  }
</script>

<button 
  class="logout-button {buttonClass}" 
  on:click={handleLogout} 
  disabled={loading}
  aria-label="Logout"
>
  {#if loading}
    <div class="spinner"></div>
  {:else}
    {buttonText}
  {/if}
</button>

<style>
  .logout-button {
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 0.5rem 1rem;
    background-color: transparent;
    color: #6b7280;
    border: 1px solid #6b7280;
    border-radius: 4px;
    cursor: pointer;
    font-size: 0.9rem;
    transition: all 0.2s;
  }
  
  .logout-button:hover:not(:disabled) {
    background-color: rgba(107, 114, 128, 0.1);
    color: #4b5563;
    border-color: #4b5563;
  }
  
  .logout-button:disabled {
    opacity: 0.7;
    cursor: not-allowed;
  }
  
  .spinner {
    width: 16px;
    height: 16px;
    border: 2px solid rgba(255, 255, 255, 0.3);
    border-radius: 50%;
    border-top-color: white;
    animation: spin 0.8s linear infinite;
  }
  
  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }
</style>
