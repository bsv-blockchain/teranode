import { toUnit } from '$lib/styles/utils/css'
import { ComponentFocusRectBorderRadius, ComponentFocusRectWidth } from './defaults'
import { comp } from './comp'
import { dropdown } from './dropdown'
import { table } from './table'
import { input } from './input'
import { link } from './link'
import { toast } from './toast'
import { msgbox } from './msgbox'
import { switchh } from './switch'
import { tab } from './tab'
import { footer } from './footer'
// import { banner } from './banner'

export const dark = {
  easing: {
    function: 'ease-in-out',
    duration: '0.2s',
  },
  focus: {
    rect: {
      color: '#ffffff', //palette.primary[500],
      width: toUnit(ComponentFocusRectWidth),
      border: {
        radius: toUnit(ComponentFocusRectBorderRadius),
      },
      padding: '0', //toUnit(0),
      bg: {
        color: '#ffffff',
      },
    },
  },
  app: {
    box: {
      sizing: 'border-box',
    },
    font: {
      family: 'Satoshi',
    },
    mono: {
      font: {
        family: 'JetBrains Mono',
      },
    },
    bg: {
      color: '#0D1117',
    },
    color: '#ffffff',
    cover: {
      bg: {
        color: 'rgba(40, 41, 51, 0.7)',
      },
    },
  },
  comp: { ...comp },
  // banner: { ...banner },
  footer: { ...footer },
  input: { ...input },
  dropdown: { ...dropdown },
  table: { ...table },
  link: { ...link },
  toast: { ...toast },
  msgbox: { ...msgbox },
  switch: { ...switchh },
  tab: { ...tab },
}
