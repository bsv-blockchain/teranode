<script lang="ts">
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/stores';
  import { login, authError, isAuthenticated } from '$internal/stores/authStore';
  import { fade } from 'svelte/transition';
  import { browser } from '$app/environment';
  import { validateUrl } from '$internal/utils/urlUtils';
  import i18n from '$internal/i18n';

  // Form fields
  let username = '';
  let password = '';
  let showPassword = false;
  let loading = false;
  let errorMessage = '';
  let csrfToken = '';
  let passwordTimeout: ReturnType<typeof setTimeout>;
  let passwordStrength = 0;
  
  // Subscribe to the auth error store
  $: errorMessage = $authError;
  $: t = $i18n.t;
  
  // Get the redirect URL from the query parameter
  let redirectUrl = '/admin';
  
  // Generate a CSRF token
  function generateCSRFToken() {
    const array = new Uint8Array(16);
    window.crypto.getRandomValues(array);
    return Array.from(array, byte => byte.toString(16).padStart(2, '0')).join('');
  }
  
  // Auto-hide password after a delay
  function startPasswordVisibilityTimer() {
    clearTimeout(passwordTimeout);
    passwordTimeout = setTimeout(() => {
      showPassword = false;
    }, 5000); // Hide password after 5 seconds
  }
  
  // Toggle password visibility
  function togglePasswordVisibility() {
    showPassword = !showPassword;
    if (showPassword) {
      startPasswordVisibilityTimer();
    } else {
      clearTimeout(passwordTimeout);
    }
  }
  
  // Check password strength
  function checkPasswordStrength(pwd: string) {
    if (!pwd) return 0;
    
    let score = 0;
    
    // Length check
    if (pwd.length >= 8) score += 1;
    if (pwd.length >= 12) score += 1;
    
    // Complexity checks
    if (/[A-Z]/.test(pwd)) score += 1;
    if (/[a-z]/.test(pwd)) score += 1;
    if (/[0-9]/.test(pwd)) score += 1;
    if (/[^A-Za-z0-9]/.test(pwd)) score += 1;
    
    // Normalize to 0-100
    return Math.min(100, Math.round((score / 6) * 100));
  }
  
  // Update password strength when password changes
  $: passwordStrength = checkPasswordStrength(password);
  
  // Get strength color based on score
  $: strengthColor = 
    passwordStrength < 30 ? 'var(--color-error)' : 
    passwordStrength < 60 ? 'var(--color-warning)' : 
    'var(--color-success)';
  
  // Handle form submission
  async function handleSubmit() {
    loading = true;
    errorMessage = '';
    
    try {
      // Attempt login with the provided credentials and CSRF token
      const success = await login(username, password, csrfToken);
      
      if (success) {
        // Validate and sanitize the redirect URL
        const validatedUrl = validateUrl(redirectUrl);
        if (validatedUrl) {
          goto(validatedUrl);
        } else {
          // Default to admin page if redirect URL is invalid
          goto('/admin');
        }
      }
    } catch (error) {
      console.error('Login error:', error);
      errorMessage = 'An unexpected error occurred. Please try again.';
    } finally {
      loading = false;
    }
  }
  
  // Navigate back to the dashboard
  function goToDashboard() {
    goto('/');
  }
  
  onMount(() => {
    // Get the redirect URL from the query parameter
    if (browser) {
      const params = new URLSearchParams($page.url.search);
      const redirect = params.get('redirect');
      if (redirect) {
        redirectUrl = redirect;
      }
      
      // Generate a CSRF token
      csrfToken = generateCSRFToken();
      
      // Check if already authenticated
      if ($isAuthenticated) {
        // Validate and sanitize the redirect URL
        const validatedUrl = validateUrl(redirectUrl);
        if (validatedUrl) {
          goto(validatedUrl);
        } else {
          // Default to admin page if redirect URL is invalid
          goto('/admin');
        }
      }
    }
  });
</script>

<svelte:head>
  <title>{t('login.title', 'Login - Teranode Admin')}</title>
</svelte:head>

<div class="login-page">
  <div class="login-container">
    <div class="login-card" in:fade={{ duration: 300, delay: 150 }}>
      <div class="login-header">
        <h1>{t('login.title', 'Teranode Admin')}</h1>
        <p>{t('login.subtitle', 'Please log in to continue')}</p>
      </div>
      
      {#if errorMessage}
        <div class="error-message" transition:fade={{ duration: 200 }}>
          {errorMessage}
        </div>
      {/if}
      
      <form 
        on:submit|preventDefault={handleSubmit} 
        id="login-form" 
        name="login-form" 
        autocomplete="on"
        method="post"
      >
        <!-- Hidden CSRF token field -->
        <input type="hidden" name="csrfToken" bind:value={csrfToken}>
        
        <div class="form-group">
          <label for="username">{t('login.username', 'Username')}</label>
          <div class="input-container">
            <input
              type="text"
              id="username"
              name="username"
              bind:value={username}
              required
              autocomplete="username webauthn"
              disabled={loading}
              placeholder="Enter username"
              aria-label="Username"
            />
          </div>
        </div>
        
        <div class="form-group">
          <label for="current-password">{t('login.password', 'Password')}</label>
          <div class="input-container password-input-container">
            <button 
              type="button" 
              class="password-toggle password-toggle-left" 
              on:click={togglePasswordVisibility}
              disabled={loading}
              aria-label={showPassword ? 'Hide password' : 'Show password'}
              tabindex="-1"
            >
              {#if showPassword}
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                  <circle cx="12" cy="12" r="3"></circle>
                  <line x1="1" y1="1" x2="23" y2="23"></line>
                </svg>
              {:else}
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"></path>
                  <circle cx="12" cy="12" r="3"></circle>
                </svg>
              {/if}
            </button>
            {#if showPassword}
              <input
                type="text"
                id="current-password"
                name="password"
                bind:value={password}
                required
                autocomplete="current-password"
                disabled={loading}
                placeholder="Enter password"
                aria-label="Password"
                class="password-input-with-left-icon"
              />
            {:else}
              <input
                type="password"
                id="current-password"
                name="password"
                bind:value={password}
                required
                autocomplete="current-password"
                disabled={loading}
                placeholder="Enter password"
                aria-label="Password"
                class="password-input-with-left-icon"
              />
            {/if}
          </div>
          
          <!-- Password strength indicator -->
          {#if password.length > 0}
            <div class="password-strength">
              <div class="strength-bar">
                <div 
                  class="strength-fill" 
                  style="width: {passwordStrength}%; background-color: {strengthColor};"
                ></div>
              </div>
              <span class="strength-text" style="color: {strengthColor};">
                {#if passwordStrength < 30}
                  {t('login.password_weak', 'Weak')}
                {:else if passwordStrength < 60}
                  {t('login.password_medium', 'Medium')}
                {:else}
                  {t('login.password_strong', 'Strong')}
                {/if}
              </span>
            </div>
          {/if}
        </div>
        
        <button type="submit" class="login-button" disabled={loading}>
          {#if loading}
            <div class="spinner"></div>
            <span>{t('login.logging_in', 'Logging in...')}</span>
          {:else}
            <span>{t('login.login_button', 'Login')}</span>
          {/if}
        </button>
      </form>
      
      <div class="back-link-container">
        <a href="/" class="back-link" on:click|preventDefault={goToDashboard}>
          {t('login.back_to_dashboard', 'Back to Dashboard')}
        </a>
      </div>
    </div>
  </div>
</div>

<style>
  .login-page {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: var(--color-background, #121212);
  }
  
  .login-container {
    width: 100%;
    max-width: 400px;
    padding: 1rem;
  }
  
  .login-card {
    background-color: var(--color-surface, #1e1e1e);
    border-radius: 8px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.2);
    padding: 2rem;
    width: 100%;
  }
  
  .login-header {
    text-align: center;
    margin-bottom: 2rem;
  }
  
  .login-header h1 {
    margin: 0;
    font-size: 1.8rem;
    color: var(--color-text, #e0e0e0);
  }
  
  .login-header p {
    margin: 0.5rem 0 0;
    color: var(--color-text-secondary, #a0a0a0);
    font-size: 0.9rem;
  }
  
  .form-group {
    margin-bottom: 1.5rem;
  }
  
  label {
    display: block;
    margin-bottom: 0.5rem;
    font-size: 0.9rem;
    color: var(--color-text-secondary, #a0a0a0);
  }
  
  .input-container {
    position: relative;
    width: 100%;
  }
  
  input {
    width: 100%;
    height: 42px;
    padding: 0.75rem;
    border: 1px solid var(--color-border, #333333);
    border-radius: 4px;
    background-color: var(--color-input-background, #252525);
    color: var(--color-text, #e0e0e0);
    font-size: 1rem;
    transition: border-color 0.2s;
    box-sizing: border-box;
  }
  
  input::placeholder {
    color: var(--color-text-muted, #666666);
  }
  
  input:focus {
    outline: none;
    border-color: var(--color-primary, #0066cc);
  }
  
  .password-input-container {
    position: relative;
  }
  
  .password-toggle {
    position: absolute;
    right: 10px;
    top: 50%;
    transform: translateY(-50%);
    background: none;
    border: none;
    color: var(--color-text-secondary, #a0a0a0);
    cursor: pointer;
    padding: 5px;
    font-size: 1rem;
    z-index: 2;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 24px;
    height: 24px;
  }
  
  .password-toggle:hover {
    color: var(--color-text, #e0e0e0);
  }
  
  .password-toggle-left {
    right: auto;
    left: 10px;
  }
  
  .password-input-with-left-icon {
    padding-left: 2.5rem;
  }
  
  .login-button {
    width: 100%;
    padding: 0.75rem;
    background-color: var(--color-primary, #0066cc);
    color: white;
    border: none;
    border-radius: 4px;
    font-size: 1rem;
    cursor: pointer;
    transition: background-color 0.2s;
    display: flex;
    justify-content: center;
    align-items: center;
    gap: 0.5rem;
  }
  
  .login-button:hover:not(:disabled) {
    background-color: var(--color-primary-dark, #0052a3);
  }
  
  .login-button:disabled {
    background-color: var(--color-disabled, #444444);
    cursor: not-allowed;
  }
  
  .error-message {
    background-color: var(--color-error-background, rgba(220, 53, 69, 0.1));
    color: var(--color-error, #dc3545);
    padding: 0.75rem;
    border-radius: 4px;
    margin-bottom: 1.5rem;
    font-size: 0.9rem;
  }
  
  .spinner {
    width: 20px;
    height: 20px;
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
  
  /* Password strength indicator */
  .password-strength {
    margin-top: 0.5rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
  }
  
  .strength-bar {
    flex-grow: 1;
    height: 4px;
    background-color: var(--color-border, #333333);
    border-radius: 2px;
    overflow: hidden;
  }
  
  .strength-fill {
    height: 100%;
    transition: width 0.3s, background-color 0.3s;
  }
  
  .strength-text {
    font-size: 0.8rem;
    min-width: 50px;
    text-align: right;
  }
  
  /* Back to dashboard link */
  .back-link-container {
    margin-top: 1.5rem;
    text-align: center;
  }
  
  .back-link {
    color: var(--color-primary, #0066cc);
    text-decoration: none;
    font-size: 0.9rem;
    transition: color 0.2s;
  }
  
  .back-link:hover {
    color: var(--color-primary-dark, #0052a3);
    text-decoration: underline;
  }
</style>
