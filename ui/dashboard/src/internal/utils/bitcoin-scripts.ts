// Bitcoin script type detection and formatting utilities

// Base58 alphabet used in Bitcoin addresses
const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'

export enum ScriptType {
  P2PKH = 'P2PKH',           // Pay to Public Key Hash
  P2SH = 'P2SH',             // Pay to Script Hash
  P2PK = 'P2PK',             // Pay to Public Key
  P2MS = 'P2MS',             // Pay to Multisig
  OP_RETURN = 'OP_RETURN',   // Data output
  UNKNOWN = 'Unknown'
}

export interface LegacyAddressVersions {
  p2pkh: number
  p2sh: number
}

// Bitcoin opcodes
const OP_FALSE = 0x00
const OP_DUP = 0x76
const OP_HASH160 = 0xa9
const OP_EQUALVERIFY = 0x88
const OP_CHECKSIG = 0xac
const OP_EQUAL = 0x87
const OP_RETURN = 0x6a
const OP_CHECKMULTISIG = 0xae

/**
 * Detects the type of a Bitcoin script
 * @param scriptHex - The script in hexadecimal format
 * @returns The detected script type
 */
export function detectScriptType(scriptHex: string): ScriptType {
  if (!scriptHex || scriptHex.length === 0) {
    return ScriptType.UNKNOWN
  }

  // Convert hex to bytes for pattern matching
  const bytes = hexToBytes(scriptHex)
  
  // P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
  if (bytes.length === 25 && 
      bytes[0] === OP_DUP && 
      bytes[1] === OP_HASH160 && 
      bytes[2] === 0x14 && // Push 20 bytes
      bytes[23] === OP_EQUALVERIFY && 
      bytes[24] === OP_CHECKSIG) {
    return ScriptType.P2PKH
  }

  // P2SH: OP_HASH160 <20 bytes> OP_EQUAL
  if (bytes.length === 23 && 
      bytes[0] === OP_HASH160 && 
      bytes[1] === 0x14 && // Push 20 bytes
      bytes[22] === OP_EQUAL) {
    return ScriptType.P2SH
  }

  // OP_RETURN: OP_RETURN <data>
  if (bytes[0] === OP_RETURN) {
    return ScriptType.OP_RETURN
  }

  // OP_FALSE OP_RETURN: OP_RETURN <data>
  if (bytes[0] === OP_FALSE && bytes[1] === OP_RETURN) {
    return ScriptType.OP_RETURN
  }

  // P2PK: <33 or 65 bytes pubkey> OP_CHECKSIG
  if ((bytes.length === 35 && bytes[0] === 0x21 && bytes[34] === OP_CHECKSIG) || // Compressed
      (bytes.length === 67 && bytes[0] === 0x41 && bytes[66] === OP_CHECKSIG)) { // Uncompressed
    return ScriptType.P2PK
  }

  // Check for multisig patterns (ends with OP_CHECKMULTISIG)
  if (bytes.length > 3 && bytes[bytes.length - 1] === OP_CHECKMULTISIG) {
    return ScriptType.P2MS
  }

  return ScriptType.UNKNOWN
}

/**
 * Converts a hex string to a byte array
 */
function hexToBytes(hex: string): number[] {
  const bytes: number[] = []
  for (let i = 0; i < hex.length; i += 2) {
    bytes.push(parseInt(hex.substr(i, 2), 16))
  }
  return bytes
}

/**
 * Formats a script to human-readable ASM format
 * @param scriptHex - The script in hexadecimal format
 * @returns The script in ASM format
 */
export function scriptToAsm(scriptHex: string): string {
  if (!scriptHex || scriptHex.length === 0) {
    return ''
  }

  const bytes = hexToBytes(scriptHex)
  const asm: string[] = []
  let i = 0

  while (i < bytes.length) {
    const opcode = bytes[i]
    
    // Handle push data operations
    if (opcode > 0 && opcode <= 75) {
      // Direct push of N bytes
      const dataLen = opcode
      i++
      const data = bytes.slice(i, i + dataLen)
      asm.push(data.map(b => b.toString(16).padStart(2, '0')).join(''))
      i += dataLen
    } else {
      // Named opcodes
      asm.push(getOpcodeName(opcode))
      i++
    }
  }

  return asm.join(' ')
}

/**
 * Gets the name of an opcode
 */
function getOpcodeName(opcode: number): string {
  const opcodes: { [key: number]: string } = {
    0x00: 'OP_0',
    0x4c: 'OP_PUSHDATA1',
    0x4d: 'OP_PUSHDATA2',
    0x4e: 'OP_PUSHDATA4',
    0x4f: 'OP_1NEGATE',
    0x51: 'OP_1',
    0x52: 'OP_2',
    0x53: 'OP_3',
    0x54: 'OP_4',
    0x55: 'OP_5',
    0x56: 'OP_6',
    0x57: 'OP_7',
    0x58: 'OP_8',
    0x59: 'OP_9',
    0x5a: 'OP_10',
    0x5b: 'OP_11',
    0x5c: 'OP_12',
    0x5d: 'OP_13',
    0x5e: 'OP_14',
    0x5f: 'OP_15',
    0x60: 'OP_16',
    0x61: 'OP_NOP',
    0x63: 'OP_IF',
    0x64: 'OP_NOTIF',
    0x67: 'OP_ELSE',
    0x68: 'OP_ENDIF',
    0x69: 'OP_VERIFY',
    0x6a: 'OP_RETURN',
    0x6b: 'OP_TOALTSTACK',
    0x6c: 'OP_FROMALTSTACK',
    0x6d: 'OP_2DROP',
    0x6e: 'OP_2DUP',
    0x6f: 'OP_3DUP',
    0x70: 'OP_2OVER',
    0x71: 'OP_2ROT',
    0x72: 'OP_2SWAP',
    0x73: 'OP_IFDUP',
    0x74: 'OP_DEPTH',
    0x75: 'OP_DROP',
    0x76: 'OP_DUP',
    0x77: 'OP_NIP',
    0x78: 'OP_OVER',
    0x79: 'OP_PICK',
    0x7a: 'OP_ROLL',
    0x7b: 'OP_ROT',
    0x7c: 'OP_SWAP',
    0x7d: 'OP_TUCK',
    0x7e: 'OP_CAT',
    0x7f: 'OP_SUBSTR',
    0x80: 'OP_LEFT',
    0x81: 'OP_RIGHT',
    0x82: 'OP_SIZE',
    0x83: 'OP_INVERT',
    0x84: 'OP_AND',
    0x85: 'OP_OR',
    0x86: 'OP_XOR',
    0x87: 'OP_EQUAL',
    0x88: 'OP_EQUALVERIFY',
    0x8b: 'OP_1ADD',
    0x8c: 'OP_1SUB',
    0x8d: 'OP_2MUL',
    0x8e: 'OP_2DIV',
    0x8f: 'OP_NEGATE',
    0x90: 'OP_ABS',
    0x91: 'OP_NOT',
    0x92: 'OP_0NOTEQUAL',
    0x93: 'OP_ADD',
    0x94: 'OP_SUB',
    0x95: 'OP_MUL',
    0x96: 'OP_DIV',
    0x97: 'OP_MOD',
    0x98: 'OP_LSHIFT',
    0x99: 'OP_RSHIFT',
    0x9a: 'OP_BOOLAND',
    0x9b: 'OP_BOOLOR',
    0x9c: 'OP_NUMEQUAL',
    0x9d: 'OP_NUMEQUALVERIFY',
    0x9e: 'OP_NUMNOTEQUAL',
    0x9f: 'OP_LESSTHAN',
    0xa0: 'OP_GREATERTHAN',
    0xa1: 'OP_LESSTHANOREQUAL',
    0xa2: 'OP_GREATERTHANOREQUAL',
    0xa3: 'OP_MIN',
    0xa4: 'OP_MAX',
    0xa5: 'OP_WITHIN',
    0xa6: 'OP_RIPEMD160',
    0xa7: 'OP_SHA1',
    0xa8: 'OP_SHA256',
    0xa9: 'OP_HASH160',
    0xaa: 'OP_HASH256',
    0xab: 'OP_CODESEPARATOR',
    0xac: 'OP_CHECKSIG',
    0xad: 'OP_CHECKSIGVERIFY',
    0xae: 'OP_CHECKMULTISIG',
    0xaf: 'OP_CHECKMULTISIGVERIFY',
    0xb0: 'OP_NOP1',
    0xb1: 'OP_CHECKLOCKTIMEVERIFY',
    0xb2: 'OP_CHECKSEQUENCEVERIFY',
    0xb3: 'OP_NOP4',
    0xb4: 'OP_NOP5',
    0xb5: 'OP_NOP6',
    0xb6: 'OP_NOP7',
    0xb7: 'OP_NOP8',
    0xb8: 'OP_NOP9',
    0xb9: 'OP_NOP10',
  }

  return opcodes[opcode] || `OP_UNKNOWN(${opcode.toString(16)})`
}

/**
 * Extracts data from an OP_RETURN output
 * @param scriptHex - The script in hexadecimal format
 * @returns The data as a string or null if not an OP_RETURN
 */
export function extractOpReturnData(scriptHex: string): string | null {
  const bytes = hexToBytes(scriptHex)

  if (bytes[0] === OP_FALSE) {
    // Skip OP_FALSE for data extraction
    bytes.shift()
  }

  if (bytes[0] !== OP_RETURN) {
    return null
  }

  // Skip OP_RETURN and any push opcodes
  let dataStart = 1
  if (bytes.length > 1) {
    if (bytes[1] <= 75) {
      // Direct push
      dataStart = 2
    } else if (bytes[1] === 0x4c) {
      // OP_PUSHDATA1
      dataStart = 3
    } else if (bytes[1] === 0x4d) {
      // OP_PUSHDATA2
      dataStart = 4
    }
  }

  // Extract the data bytes
  const dataBytes = bytes.slice(dataStart)
  
  // Try to decode as UTF-8
  try {
    const decoder = new TextDecoder('utf-8', { fatal: true })
    const uint8Array = new Uint8Array(dataBytes)
    return decoder.decode(uint8Array)
  } catch {
    // If not valid UTF-8, return as hex
    return dataBytes.map(b => b.toString(16).padStart(2, '0')).join('')
  }
}

/**
 * Gets a human-readable description of a script type
 */
export function getScriptTypeDescription(type: ScriptType): string {
  const descriptions: { [key in ScriptType]: string } = {
    [ScriptType.P2PKH]: 'Pay to Public Key Hash',
    [ScriptType.P2SH]: 'Pay to Script Hash',
    [ScriptType.P2PK]: 'Pay to Public Key',
    [ScriptType.P2MS]: 'Pay to Multisig',
    [ScriptType.OP_RETURN]: 'Data Output (OP_RETURN)',
    [ScriptType.UNKNOWN]: 'Unknown Script Type'
  }
  return descriptions[type]
}

/**
 * Base58 encoding implementation for Bitcoin addresses
 */
function base58Encode(bytes: Uint8Array): string {
  const digits = [0]
  
  for (let i = 0; i < bytes.length; i++) {
    let carry = bytes[i]
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] * 256
      digits[j] = carry % 58
      carry = Math.floor(carry / 58)
    }
    while (carry > 0) {
      digits.push(carry % 58)
      carry = Math.floor(carry / 58)
    }
  }
  
  // Convert digits to base58 string
  let result = ''
  
  // Add leading 1s for leading zero bytes
  for (let i = 0; i < bytes.length && bytes[i] === 0; i++) {
    result += '1'
  }
  
  // Convert digits to characters
  for (let i = digits.length - 1; i >= 0; i--) {
    result += BASE58_ALPHABET[digits[i]]
  }
  
  return result
}

/**
 * Calculates double SHA-256 hash (synchronous version using Web Crypto API)
 * Note: This is an async function wrapped to work synchronously for simplicity
 */
async function doubleSha256(data: Uint8Array): Promise<Uint8Array> {
  // Ensure we pass an ArrayBuffer (not ArrayBufferLike) to satisfy BufferSource
  const input: ArrayBuffer =
    data.byteOffset === 0 && data.byteLength === data.buffer.byteLength
      ? (data.buffer as ArrayBuffer)
      : (data.slice().buffer as ArrayBuffer)

  const hash1 = await crypto.subtle.digest('SHA-256', input)
  const hash2 = await crypto.subtle.digest('SHA-256', hash1)
  return new Uint8Array(hash2)
}

/**
 * Extracts a Bitcoin address from a script
 * @param scriptHex - The script in hexadecimal format
 * @param scriptType - The detected script type
 * @returns The Bitcoin address or null if not applicable
 */
export async function extractAddress(
  scriptHex: string,
  scriptType: ScriptType,
  versionsOverride?: Partial<LegacyAddressVersions>,
): Promise<string | null> {
  const bytes = hexToBytes(scriptHex)

  const versions: LegacyAddressVersions = {
    p2pkh: 0x00,
    p2sh: 0x05,
  }

  if (versionsOverride) {
    if (typeof versionsOverride.p2pkh === 'number') {
      versions.p2pkh = versionsOverride.p2pkh
    }
    if (typeof versionsOverride.p2sh === 'number') {
      versions.p2sh = versionsOverride.p2sh
    }
  }
  
  if (scriptType === ScriptType.P2PKH) {
    // P2PKH: Extract the 20-byte public key hash
    // Script structure: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
    if (bytes.length === 25 && bytes[2] === 0x14) {
      const pubKeyHash = bytes.slice(3, 23)
      
      // Add version byte (0x00 for mainnet P2PKH)
      const versionedHash = new Uint8Array(21)
      versionedHash[0] = versions.p2pkh
      versionedHash.set(pubKeyHash, 1)
      
      // Calculate checksum (first 4 bytes of double SHA-256)
      const checksum = (await doubleSha256(versionedHash)).slice(0, 4)
      
      // Combine version + hash + checksum
      const addressBytes = new Uint8Array(25)
      addressBytes.set(versionedHash, 0)
      addressBytes.set(checksum, 21)
      
      // Base58 encode
      return base58Encode(addressBytes)
    }
  } else if (scriptType === ScriptType.P2SH) {
    // P2SH: Extract the 20-byte script hash
    // Script structure: OP_HASH160 <20 bytes> OP_EQUAL
    if (bytes.length === 23 && bytes[1] === 0x14) {
      const scriptHash = bytes.slice(2, 22)
      
      // Add version byte (0x05 for mainnet P2SH)
      const versionedHash = new Uint8Array(21)
      versionedHash[0] = versions.p2sh
      versionedHash.set(scriptHash, 1)
      
      // Calculate checksum
      const checksum = (await doubleSha256(versionedHash)).slice(0, 4)
      
      // Combine version + hash + checksum
      const addressBytes = new Uint8Array(25)
      addressBytes.set(versionedHash, 0)
      addressBytes.set(checksum, 21)
      
      // Base58 encode
      return base58Encode(addressBytes)
    }
  }
  
  return null
}

/**
 * Synchronous version that extracts just the hash without full address encoding
 * Use this when you need immediate results
 */
export function extractPubKeyHash(scriptHex: string, scriptType: ScriptType): string | null {
  const bytes = hexToBytes(scriptHex)
  
  if (scriptType === ScriptType.P2PKH) {
    // P2PKH: Extract the 20-byte public key hash
    if (bytes.length === 25 && bytes[2] === 0x14) {
      const pubKeyHash = bytes.slice(3, 23)
      return pubKeyHash.map(b => b.toString(16).padStart(2, '0')).join('')
    }
  } else if (scriptType === ScriptType.P2SH) {
    // P2SH: Extract the 20-byte script hash
    if (bytes.length === 23 && bytes[1] === 0x14) {
      const scriptHash = bytes.slice(2, 22)
      return scriptHash.map(b => b.toString(16).padStart(2, '0')).join('')
    }
  }
  
  return null
}