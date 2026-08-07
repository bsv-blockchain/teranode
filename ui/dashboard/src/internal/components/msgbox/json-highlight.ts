/**
 * Renders a message as syntax-highlighted JSON for the raw-JSON view.
 *
 * The messages fed to this module come straight off the /p2p-ws websocket, so
 * every string in them is peer-controlled and must be treated as hostile. The
 * result is interpolated with Svelte's {@html}, which does no escaping of its
 * own, so escaping happens here.
 */

/**
 * Escapes the characters that let text become markup.
 *
 * `"` is deliberately left as-is: the escaped text is only ever interpolated
 * into element content, never into an attribute value, so a quote there is
 * inert. Escaping `&` first is what stops `&lt;` in the input from being
 * decoded back into `<` by the browser.
 */
export function escapeHtml(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

/**
 * Matches one JSON token: a quoted string (optionally followed by the `:` that
 * makes it a property name), a literal, or a number.
 *
 * The string alternative comes first and consumes escape pairs, so a token-like
 * substring inside a string value - `"a: true"`, `"x\": 1"` - is swallowed by
 * the surrounding string rather than matched on its own.
 */
const JSON_TOKEN = /("(?:\\.|[^"\\])*")(\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g

/**
 * Wraps JSON tokens in styling spans.
 *
 * This walks the raw JSON once and escapes every fragment as it is emitted, so
 * the spans are placed structurally rather than by rewriting text that already
 * contains markup. That is what makes the invariant hold: the only tags in the
 * output are the ones written here, and no attacker-controlled text can ever
 * land inside one of their class attributes.
 */
function highlightJSON(json: string): string {
  let out = ''
  let last = 0

  for (const match of json.matchAll(JSON_TOKEN)) {
    const [token, quoted, colon] = match
    const start = match.index

    out += escapeHtml(json.slice(last, start))

    if (quoted !== undefined) {
      out +=
        colon === undefined
          ? `<span class="json-string">${escapeHtml(quoted)}</span>`
          : `<span class="json-key">${escapeHtml(quoted)}</span>${escapeHtml(colon)}`
    } else if (token === 'true' || token === 'false') {
      out += `<span class="json-boolean">${token}</span>`
    } else if (token === 'null') {
      out += `<span class="json-null">${token}</span>`
    } else {
      out += `<span class="json-number">${token}</span>`
    }

    last = start + token.length
  }

  return out + escapeHtml(json.slice(last))
}

export function formatJSON(obj: unknown): string {
  let json: string | undefined

  try {
    json = JSON.stringify(obj, null, 2)
  } catch (e) {
    return '{}'
  }

  // JSON.stringify returns undefined for undefined and for functions/symbols.
  if (json === undefined) {
    return '{}'
  }

  return highlightJSON(json)
}
