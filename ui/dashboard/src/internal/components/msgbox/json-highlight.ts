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
 * `"` is deliberately left as-is. The escaped text is only ever interpolated
 * into element content, never into an attribute value, so a quote there is
 * inert - and keeping it lets highlightJSON below still recognise JSON string
 * delimiters. Escaping `&` first is what stops `&lt;` in the input from being
 * decoded back into `<` by the browser.
 */
export function escapeHtml(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}

/**
 * Wraps JSON tokens in styling spans. Only ever called on escaped text, which
 * is what makes it safe: with no `<` or `&` left in the input, the only tags in
 * the output are the ones added here.
 */
function highlightJSON(escaped: string): string {
  return (
    escaped
      // Strings (but not property names)
      .replace(/: "([^"]*)"/g, ': <span class="json-string">"$1"</span>')
      // Numbers
      .replace(/: (\d+)/g, ': <span class="json-number">$1</span>')
      // Booleans
      .replace(/: (true|false)/g, ': <span class="json-boolean">$1</span>')
      // Null
      .replace(/: (null)/g, ': <span class="json-null">$1</span>')
      // Property names
      .replace(/"([^"]+)":/g, '<span class="json-key">"$1"</span>:')
  )
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

  return highlightJSON(escapeHtml(json))
}
