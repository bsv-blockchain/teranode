import { app } from '../components/app'
import { component } from '../components/component'
import { input } from '../components/input'
import { dropdown } from '../components/dropdown'
import { checkbox } from '../components/checkbox'
import { radio } from '../components/radio'
import { switchh } from '../components/switch'
import { tab } from '../components/tab'
import { link } from '../components/link'
import { header } from '../components/header'
import { table } from '../components/table'
import { toast } from '../components/toast'

export const defaults = {
  app: { ...app },
  comp: { ...component },
  input: { ...input },
  dropdown: { ...dropdown },
  checkbox: { ...checkbox },
  radio: { ...radio },
  switch: { ...switchh },
  tab: { ...tab },
  link: { ...link },
  header: { ...header },
  table: { ...table },
  toast: { ...toast },
}
