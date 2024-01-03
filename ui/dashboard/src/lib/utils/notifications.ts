import { toast } from '@zerodevx/svelte-toast'
import { Toast } from '$lib/components'
import { ToastStatus } from '$lib/components/toast/types'
import { t } from 'i18next'

export const success = (m, opts: any = {}) => {
  const { theme = {}, ...rest } = opts
  toast.push({
    component: {
      src: Toast,
      props: {
        status: ToastStatus.success,
        title: t(`notifications.title.${ToastStatus.success}`),
        message: m,
      },
    },
    theme: {
      // '--toastBarBackground': '#6EC492',
      ...theme,
    },
    ...rest,
  })
}

export const failure = (m, opts: any = {}) => {
  const { theme = {}, ...rest } = opts
  toast.push({
    component: {
      src: Toast,
      props: {
        status: ToastStatus.failure,
        title: t(`notifications.title.${ToastStatus.failure}`),
        message: m,
      },
    },
    theme: {
      // '--toastBarBackground': '#FF344C',
      ...theme,
    },
    ...rest,
  })
}
