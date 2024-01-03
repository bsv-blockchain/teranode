// See: https://kit.svelte.dev/docs/errors

import { sequence } from '@sveltejs/kit/hooks'
import { handleErrorWithSentry, sentryHandle } from '@sentry/sveltekit'
import * as Sentry from '@sentry/sveltekit'

Sentry.init({
  dsn: 'https://90ee6fb8fad41f5cf02f16e6913d5069@o4505918315626496.ingest.sentry.io/4505918315692032',
  tracesSampleRate: 1.0,
})

// If you have custom handlers, make sure to place them after `sentryHandle()` in the `sequence` function.
export const handle = sequence(sentryHandle())

// If you have a custom error handler, pass it to `handleErrorWithSentry`
export const handleError = handleErrorWithSentry()
