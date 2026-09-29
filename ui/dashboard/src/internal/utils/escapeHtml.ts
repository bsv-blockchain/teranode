/**
 * Escapes the characters that let text become markup.
 *
 * `"` is deliberately left as-is: the escaped text is only ever interpolated
 * into element content, never into an attribute value, so a quote there is
 * inert. Escaping `&` first is what stops `&lt;` in the input from being
 * decoded back into `<` by the browser.
 *
 * If a future caller places an escaped value inside an attribute value, that
 * caller must escape quotes too - this function on its own is not enough there.
 */
export function escapeHtml(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
}
