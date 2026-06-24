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
    border: {
      color: 'rgba(255, 255, 255, 0.12)',
    },
    subtle: {
      bg: {
        color: 'rgba(255, 255, 255, 0.08)',
      },
    },
    overlay: {
      color: 'rgba(255, 255, 255, 0.10)',
      strong: {
        color: 'rgba(255, 255, 255, 0.18)',
      },
    },
  },
  comp: { ...comp },
  json: {
    display: {
      color: '#b4b4b4',
      key: { color: '#a8c0ff' },
      string: { color: '#98c379' },
      number: { color: '#d19a66' },
      boolean: { color: '#56b6c2' },
      null: { color: '#abb2bf' },
    },
    tree: {
      string: { color: '#15b241' },
      string2: { color: '#9917ff' },
      boolean: { color: '#1a6bd4' },
    },
  },
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
  toggle: {
    bg: {
      color: '#33373c',
    },
  },
}
