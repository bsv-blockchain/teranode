# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Teranode UI Dashboard (SvelteKit application)

Build and run development server:
```bash
npm install --prefix ./ui/dashboard
npm run dev --prefix ./ui/dashboard
```

Build for production:
```bash
npm run build --prefix ./ui/dashboard
```

Run tests:
```bash
npm run test --prefix ./ui/dashboard
```

Lint and format:
```bash
npm run lint --prefix ./ui/dashboard
npm run format --prefix ./ui/dashboard
```

Check TypeScript types:
```bash
npm run check --prefix ./ui/dashboard
```

## High-Level Architecture

### Teranode UI Dashboard

The dashboard is a SvelteKit application that provides a web interface for monitoring and interacting with Teranode. Key architectural aspects:

1. **Framework**: Built with SvelteKit using TypeScript
2. **Structure**:
   - `/src/lib/` - Shared library components intended for potential extraction into a separate package
   - `/src/internal/` - Project-specific components and utilities
   - `/src/routes/` - Page routes following SvelteKit's file-based routing
   - Static adapter configured for single-page application with `index.html` fallback

3. **Key Features**:
   - Real-time monitoring of blockchain state, network peers, and P2P connections
   - Block and transaction viewer with search capabilities
   - Admin interface for node management
   - WebSocket integration for live updates
   - i18n support via svelte-i18next
   - D3.js and ECharts for data visualization

4. **State Management**:
   - Svelte stores for reactive state management
   - Authentication state managed via `authStore`
   - WebSocket listeners managed via `listenerStore`
   - Node and P2P data managed via dedicated stores

5. **API Integration**:
   - REST API endpoints under `/api/`
   - WebSocket configuration endpoint
   - Authentication endpoints (login/logout/check)

6. **Build Configuration**:
   - Vite as build tool
   - Static adapter for deployment
   - Source maps enabled for debugging
   - Custom path alias `$internal` for internal components