<script lang="ts">
  import { onMount } from 'svelte'
  import PageWithMenu from '$internal/components/page/template/menu/index.svelte'
  import { getSettings, type SettingMetadata, type SettingsResponse } from '$internal/api'
  import i18n from '$internal/i18n'
  import { Icon, TextInput } from '$lib/components'
  import Markdown from 'svelte-exmarkdown'

  let settings: SettingMetadata[] = []
  let categories: string[] = []
  let loading = true
  let error: string | null = null
  let version = ''
  let commit = ''
  let total = 0
  let filtered = 0

  // Filter state
  let selectedCategory = ''
  let searchQuery = ''
  let searchTimeout: ReturnType<typeof setTimeout> | null = null
  // Use an object for reactivity instead of Set
  let expandedKeys: Record<string, boolean> = {}

  // Get translation function
  const t = $i18n.t

  async function fetchSettings() {
    loading = true
    error = null

    try {
      const params: { category?: string; search?: string } = {}
      if (selectedCategory) {
        params.category = selectedCategory
      }
      if (searchQuery) {
        params.search = searchQuery
      }

      const result = await getSettings(params)

      if (!result.ok) {
        throw new Error(result.error?.message || 'Failed to fetch settings')
      }

      const data = result.data as SettingsResponse
      settings = data.settings || []
      categories = data.categories || []
      version = data.version || ''
      commit = data.commit || ''
      total = data.total || 0
      filtered = data.filtered || 0

      // Log warning if duplicates are detected
      const seen = new Set<string>()
      const duplicates = new Set<string>()
      settings.forEach(s => {
        const key = `${s.category}-${s.key}`
        if (seen.has(key)) {
          duplicates.add(key)
        }
        seen.add(key)
      })
      if (duplicates.size > 0) {
        console.warn('Duplicate settings detected from backend:', Array.from(duplicates))
      }
    } catch (err) {
      console.error('Error fetching settings:', err)
      error = err instanceof Error ? err.message : String(err)
    } finally {
      loading = false
    }
  }

  function handleCategoryChange(category: string) {
    selectedCategory = category
    fetchSettings()
  }

  function handleSearchInput() {
    // Debounce search
    if (searchTimeout) {
      clearTimeout(searchTimeout)
    }

    searchTimeout = setTimeout(() => {
      fetchSettings()
    }, 300)
  }

  function clearFilters() {
    selectedCategory = ''
    searchQuery = ''
    fetchSettings()
  }

  function isModified(setting: SettingMetadata): boolean {
    return setting.currentValue !== setting.defaultValue && setting.currentValue !== ''
  }

  function toggleExpanded(key: string) {
    expandedKeys[key] = !expandedKeys[key]
    expandedKeys = expandedKeys // Trigger reactivity
  }

  function formatValue(value: string, type: string): string {
    if (!value) return '-'
    if (value === '********') return '••••••••'
    if (value.startsWith('[') && value.endsWith(']')) return value

    // Truncate long values
    if (value.length > 60) {
      return value.substring(0, 57) + '...'
    }

    return value
  }

  function getTypeColor(type: string): string {
    switch (type) {
      case 'bool':
        return '#10b981'
      case 'int':
      case 'uint32':
      case 'float64':
        return '#3b82f6'
      case 'duration':
        return '#8b5cf6'
      case 'url':
        return '#f59e0b'
      case '[]string':
        return '#ec4899'
      default:
        return '#6b7280'
    }
  }

  onMount(() => {
    fetchSettings()
  })
</script>

<PageWithMenu>
  <div class="settings-container">
    <header class="settings-header">
      <div class="header-left">
        <h1>{t('page.settings.title', 'Settings Reference')}</h1>
        {#if version || commit}
          <span class="version-info">
            {#if version}v{version}{/if}
            {#if commit}({commit.substring(0, 7)}){/if}
          </span>
        {/if}
      </div>
      <div class="header-stats">
        {#if !loading}
          <span class="stat-item">
            <span class="stat-value">{filtered}</span>
            <span class="stat-label">of {total} settings</span>
          </span>
        {/if}
      </div>
    </header>

    <div class="settings-controls">
      <div class="search-box">
        <Icon name="icon-search-line" size={18} />
        <input
          type="text"
          placeholder={t('page.settings.search-placeholder', 'Search settings...')}
          on:input={handleSearchInput}
          bind:value={searchQuery}
          class="search-input"
        />
        {#if searchQuery}
          <button class="clear-search" on:click={() => { searchQuery = ''; fetchSettings(); }}>
            <Icon name="icon-close-line" size={16} />
          </button>
        {/if}
      </div>

      <div class="category-filter">
        <button
          class="category-btn"
          class:active={selectedCategory === ''}
          on:click={() => handleCategoryChange('')}
        >
          {t('page.settings.all-categories', 'All Categories')}
        </button>
        {#each categories as category}
          <button
            class="category-btn"
            class:active={selectedCategory === category}
            on:click={() => handleCategoryChange(category)}
          >
            {category}
          </button>
        {/each}
      </div>
    </div>

    {#if loading}
      <div class="loading-container">
        <div class="spinner"></div>
        <p>Loading settings...</p>
      </div>
    {:else if error}
      <div class="error-container">
        <Icon name="icon-warning-line" size={48} />
        <p class="error-message">{error}</p>
        <button class="retry-btn" on:click={fetchSettings}>
          <Icon name="icon-refresh-line" size={18} />
          <span>Retry</span>
        </button>
      </div>
    {:else if settings.length === 0}
      <div class="empty-container">
        <Icon name="icon-search-line" size={48} />
        <p>No settings found matching your criteria</p>
        <button class="clear-btn" on:click={clearFilters}>
          Clear filters
        </button>
      </div>
    {:else}
      <div class="settings-table-wrapper">
        <table class="settings-table">
          <thead>
            <tr>
              <th class="col-key">{t('page.settings.col-key', 'Config Key')}</th>
              <th class="col-type">Type</th>
              <th class="col-default">{t('page.settings.col-default', 'Default')}</th>
              <th class="col-current">{t('page.settings.col-current', 'Current')}</th>
              <th class="col-description">{t('page.settings.col-description', 'Description')}</th>
            </tr>
          </thead>
          <tbody>
            {#each settings as setting, index (`${setting.category}-${setting.key}-${index}`)}
              {@const uniqueKey = `${setting.category}-${setting.key}`}
              <tr class:modified={isModified(setting)} class:expanded={expandedKeys[uniqueKey]}>
                <td class="col-key">
                  <div class="key-cell">
                    <span class="setting-key">{setting.key}</span>
                    <span class="setting-name">{setting.name}</span>
                    {#if isModified(setting)}
                      <span class="modified-badge">{t('page.settings.badge-modified', 'Modified')}</span>
                    {/if}
                    {#if setting.longDescription}
                      <button
                        class="expand-btn"
                        on:click={() => toggleExpanded(uniqueKey)}
                        aria-expanded={expandedKeys[uniqueKey] || false}
                      >
                        <Icon name={expandedKeys[uniqueKey] ? 'icon-arrow-up-s-line' : 'icon-arrow-down-s-line'} size={16} />
                        <span>{expandedKeys[uniqueKey] ? t('page.settings.show-less', 'Show less') : t('page.settings.show-more', 'Show more details')}</span>
                      </button>
                    {/if}
                  </div>
                </td>
                <td class="col-type">
                  <span class="type-badge" style="background-color: {getTypeColor(setting.type)}20; color: {getTypeColor(setting.type)}">
                    {setting.type}
                  </span>
                </td>
                <td class="col-default">
                  <code class="value-code">{formatValue(setting.defaultValue, setting.type)}</code>
                </td>
                <td class="col-current">
                  <code class="value-code" class:modified-value={isModified(setting)}>
                    {formatValue(setting.currentValue, setting.type)}
                  </code>
                </td>
                <td class="col-description">
                  <div class="description-cell">
                    <p class="description-text">{setting.description}</p>
                    {#if setting.usageHint}
                      <p class="usage-hint">
                        <Icon name="icon-info-line" size={14} />
                        {setting.usageHint}
                      </p>
                    {/if}
                  </div>
                </td>
              </tr>
              {#if expandedKeys[uniqueKey] && setting.longDescription}
                <tr class="long-description-row">
                  <td colspan="5" class="long-description-cell">
                    <div class="long-description">
                      <Markdown md={setting.longDescription} />
                    </div>
                  </td>
                </tr>
              {/if}
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</PageWithMenu>

<style>
  .settings-container {
    padding: 2rem;
    max-width: 1600px;
    margin: 0 auto;
    color: #e9ecef;
  }

  .settings-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 2rem;
    padding-bottom: 1.5rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  }

  .header-left {
    display: flex;
    align-items: baseline;
    gap: 1rem;
  }

  .settings-header h1 {
    font-size: 2rem;
    font-weight: 700;
    color: #f8f9fa;
    margin: 0;
    letter-spacing: 0.01em;
  }

  .version-info {
    font-size: 0.875rem;
    color: #6b7280;
    font-family: monospace;
  }

  .header-stats {
    display: flex;
    gap: 1.5rem;
  }

  .stat-item {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  .stat-value {
    font-size: 1.5rem;
    font-weight: 600;
    color: #3b82f6;
  }

  .stat-label {
    font-size: 0.875rem;
    color: #6b7280;
  }

  .settings-controls {
    margin-bottom: 1.5rem;
    display: flex;
    flex-direction: column;
    gap: 1rem;
  }

  .search-box {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    background-color: rgba(30, 30, 30, 0.6);
    border: 1px solid #4b5563;
    border-radius: 0.5rem;
    padding: 0.75rem 1rem;
    max-width: 400px;
    transition: all 0.2s ease;
  }

  .search-box:focus-within {
    border-color: #3b82f6;
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.3);
  }

  .search-input {
    flex: 1;
    background: transparent;
    border: none;
    color: #e9ecef;
    font-size: 1rem;
    outline: none;
  }

  .search-input::placeholder {
    color: #6b7280;
  }

  .clear-search {
    background: transparent;
    border: none;
    color: #6b7280;
    cursor: pointer;
    padding: 0.25rem;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 0.25rem;
    transition: all 0.2s ease;
  }

  .clear-search:hover {
    color: #e9ecef;
    background-color: rgba(255, 255, 255, 0.1);
  }

  .category-filter {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .category-btn {
    padding: 0.5rem 1rem;
    background-color: rgba(30, 30, 30, 0.6);
    border: 1px solid #4b5563;
    border-radius: 0.375rem;
    color: #9ca3af;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
  }

  .category-btn:hover {
    border-color: #3b82f6;
    color: #e9ecef;
  }

  .category-btn.active {
    background-color: #3b82f6;
    border-color: #3b82f6;
    color: white;
  }

  .loading-container,
  .error-container,
  .empty-container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 4rem 2rem;
    text-align: center;
    background-color: rgba(33, 37, 41, 0.6);
    border-radius: 0.75rem;
  }

  .spinner {
    border: 4px solid rgba(255, 255, 255, 0.1);
    width: 48px;
    height: 48px;
    border-radius: 50%;
    border-left-color: #3b82f6;
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
  }

  .loading-container p,
  .error-container p,
  .empty-container p {
    margin-top: 1rem;
    color: #9ca3af;
  }

  .error-message {
    color: #ef4444 !important;
  }

  .retry-btn,
  .clear-btn {
    margin-top: 1rem;
    padding: 0.75rem 1.5rem;
    background-color: #3b82f6;
    border: none;
    border-radius: 0.5rem;
    color: white;
    font-weight: 500;
    cursor: pointer;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    transition: all 0.2s ease;
  }

  .retry-btn:hover,
  .clear-btn:hover {
    background-color: #2563eb;
  }

  .settings-table-wrapper {
    background-color: rgba(33, 37, 41, 0.6);
    border-radius: 0.75rem;
    overflow-x: auto;
    overflow-y: visible;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  }

  .settings-table {
    width: 100%;
    min-width: 900px;
    border-collapse: collapse;
    font-size: 0.9rem;
  }

  .settings-table thead {
    background-color: rgba(33, 37, 41, 0.8);
    position: sticky;
    top: 0;
    z-index: 10;
  }

  .settings-table th {
    text-align: left;
    padding: 1rem 1.25rem;
    font-weight: 600;
    color: #9ca3af;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    font-size: 0.8rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  .settings-table td {
    padding: 1rem 1.25rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    vertical-align: top;
  }

  .settings-table tr:hover {
    background-color: rgba(59, 130, 246, 0.05);
  }

  .settings-table tr.modified {
    background-color: rgba(245, 158, 11, 0.05);
  }

  .settings-table tr.modified:hover {
    background-color: rgba(245, 158, 11, 0.1);
  }

  .col-key {
    width: 25%;
    min-width: 200px;
  }

  .col-type {
    width: 8%;
    min-width: 80px;
  }

  .col-default,
  .col-current {
    width: 15%;
    min-width: 120px;
  }

  .col-description {
    width: 37%;
  }

  .key-cell {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .setting-key {
    font-family: monospace;
    font-weight: 600;
    color: #e9ecef;
    font-size: 0.875rem;
  }

  .setting-name {
    font-size: 0.8rem;
    color: #6b7280;
  }

  .modified-badge {
    display: inline-block;
    padding: 0.125rem 0.5rem;
    background-color: rgba(245, 158, 11, 0.2);
    color: #f59e0b;
    border-radius: 0.25rem;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    margin-top: 0.25rem;
    width: fit-content;
  }

  .type-badge {
    display: inline-block;
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    font-size: 0.75rem;
    font-weight: 500;
    font-family: monospace;
  }

  .value-code {
    font-family: monospace;
    font-size: 0.8rem;
    color: #9ca3af;
    background-color: rgba(0, 0, 0, 0.2);
    padding: 0.25rem 0.5rem;
    border-radius: 0.25rem;
    word-break: break-all;
    display: inline-block;
    max-width: 100%;
  }

  .value-code.modified-value {
    color: #f59e0b;
    background-color: rgba(245, 158, 11, 0.1);
  }

  .description-cell {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .description-text {
    color: #d1d5db;
    line-height: 1.5;
    margin: 0;
  }

  .usage-hint {
    display: flex;
    align-items: flex-start;
    gap: 0.5rem;
    color: #6b7280;
    font-size: 0.8rem;
    font-style: italic;
    margin: 0;
    padding: 0.5rem;
    background-color: rgba(107, 114, 128, 0.1);
    border-radius: 0.25rem;
  }

  .expand-btn {
    display: inline-flex;
    align-items: center;
    gap: 0.375rem;
    padding: 0.375rem 0.75rem;
    background-color: transparent;
    border: 1px solid #4b5563;
    border-radius: 0.375rem;
    color: #9ca3af;
    font-size: 0.75rem;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s ease;
    margin-top: 0.5rem;
  }

  .expand-btn:hover {
    border-color: #3b82f6;
    color: #e9ecef;
    background-color: rgba(59, 130, 246, 0.1);
  }

  .long-description-row {
    background-color: rgba(30, 30, 30, 0.8);
  }

  .long-description-row:hover {
    background-color: rgba(30, 30, 30, 0.8);
  }

  .settings-table tr.modified + .long-description-row {
    background-color: rgba(245, 158, 11, 0.05);
  }

  .long-description-cell {
    padding: 0 !important;
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
  }

  .long-description {
    padding: 1.5rem 1.25rem;
    border-left: 3px solid #3b82f6;
    background-color: rgba(30, 30, 30, 0.6);
  }

  .long-description :global(p) {
    margin: 0 0 1rem 0;
    color: #d1d5db;
    font-size: 0.85rem;
    line-height: 1.7;
  }

  .long-description :global(p:last-child) {
    margin-bottom: 0;
  }

  .long-description :global(code) {
    font-family: monospace;
    font-size: 0.8rem;
    color: #f59e0b;
    background-color: rgba(0, 0, 0, 0.3);
    padding: 0.125rem 0.375rem;
    border-radius: 0.25rem;
  }

  .long-description :global(pre) {
    background-color: rgba(0, 0, 0, 0.3);
    padding: 1rem;
    border-radius: 0.375rem;
    overflow-x: auto;
    margin: 0.5rem 0;
  }

  .long-description :global(pre code) {
    background-color: transparent;
    padding: 0;
  }

  .long-description :global(ul),
  .long-description :global(ol) {
    margin: 0.5rem 0;
    padding-left: 1.5rem;
    color: #d1d5db;
  }

  .long-description :global(li) {
    margin: 0.25rem 0;
    line-height: 1.7;
  }

  .long-description :global(strong) {
    color: #e9ecef;
    font-weight: 600;
  }

  .long-description :global(em) {
    font-style: italic;
  }

  .long-description :global(a) {
    color: #3b82f6;
    text-decoration: none;
  }

  .long-description :global(a:hover) {
    text-decoration: underline;
  }

  .long-description :global(h1),
  .long-description :global(h2),
  .long-description :global(h3),
  .long-description :global(h4),
  .long-description :global(h5),
  .long-description :global(h6) {
    color: #f8f9fa;
    margin: 1rem 0 0.5rem 0;
    font-weight: 600;
  }

  .long-description :global(h1:first-child),
  .long-description :global(h2:first-child),
  .long-description :global(h3:first-child),
  .long-description :global(h4:first-child),
  .long-description :global(h5:first-child),
  .long-description :global(h6:first-child) {
    margin-top: 0;
  }

  .long-description :global(blockquote) {
    border-left: 3px solid #4b5563;
    padding-left: 1rem;
    margin: 0.5rem 0;
    color: #9ca3af;
    font-style: italic;
  }

  /* Responsive adjustments */
  @media (max-width: 1200px) {
    .settings-container {
      padding: 1.5rem;
    }

    .settings-table th,
    .settings-table td {
      padding: 0.75rem 1rem;
    }
  }

  @media (max-width: 768px) {
    .settings-header {
      flex-direction: column;
      align-items: flex-start;
      gap: 1rem;
    }

    .search-box {
      max-width: 100%;
      width: 100%;
    }
  }
</style>
