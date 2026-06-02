import type { Page } from '@playwright/test'

// Intercepts any WebSocket connection the dashboard opens. Accepts the
// connection, swallows any send (so subscribe messages don't surface as
// "unhandled"), and never pushes data. Goal: prevent reconnect loops from
// flooding the console; we are not exercising live update paths in smoke.
export async function installWsMocks(page: Page): Promise<void> {
  await page.routeWebSocket(/.*/, (ws) => {
    ws.onMessage(() => {
      // intentionally ignore — smoke does not assert WS payloads
    })
  })
}
