export async function copyTextToClipboardVanilla(text: string) {
  try {
    await navigator.clipboard.writeText(text)
    return { ok: true }
  } catch (error) {
    return { ok: false, error }
  }
}
