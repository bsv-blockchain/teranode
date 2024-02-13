// See: https://medium.com/@ackhor/ch12-something-on-bits-target-difficulty-f863134061fb
import { lpad, rpad } from './strings'

export const getTargetHex = (targetBits: string) => {
  if (targetBits.substring(0, 2) !== '0x') {
    targetBits = '0x' + targetBits
  }
  const digitLength = parseInt(targetBits.substring(0, 4))
  const coefficient = targetBits.substring(4, 10)
  const right = rpad(coefficient, digitLength * 2, '0')
  return '0x' + lpad(right, 64, '0')
}

export const getTargetDecimal = (targetBits: string) => {
  return parseInt(getTargetHex(targetBits))
}

export const targetMaxBits = '0x1d00ffff'
export const targetMaxDecimal = getTargetDecimal(targetMaxBits)

export const getDifficultyFromBits = (targetCurrentBits: string) => {
  return targetMaxDecimal / getTargetDecimal(targetCurrentBits)
}
