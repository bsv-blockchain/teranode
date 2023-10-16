SUTOS - Scaleable Unspent Transaction Output Store

Bitcoin uses a UTXO (Unspent Transaction Output) model to store the state of the blockchain. This means that every transaction output is stored in the blockchain and can be spent in a later transaction. This is a very simple model, but it has some drawbacks:

The UTXO set is very large and needs to be stored in memory. This makes it hard to scale Bitcoin.

UTXO sets are immutable. This means that if you want to change the UTXO set, you need to download the whole UTXO set and reprocess all transactions.

SUTOS is a scalable UTXO store that uses a sharded database scheme to allow for massive, horizontal scaling. It is written in Go and uses XXXX cluster as an underlying store.

SUTOS is currently in development and not ready for production use.

License

SUTOS is licensed under the MIT License. See LICENSE for the full license text.

Copyright (c) 2023 The SUTOS developers.


## Overview

When validating a Bitcoin transaction, it is necessary to know details of all the previous transaction outputs that are being used as inputs in this transaction.  A UTXO is made up of 4 elements:

1. The transaction hash of the transaction that created the output
2. The index of the output in the transaction
3. The amount of the output in Satoshis
4. The public key script that locks the output

All this information is required to validate the unlocking script is able to spend the output.  The transaction hash of the previous transaction and the index of the output in that transaction are included in a transaction input.  The amount of the output and the public key script are included in the previous transaction and need to be retrieved before the transaction can be fully validated.  With SUTOS, we rely on the Transaction Extended Format [BIP-239](BIP-239) which allows the sender of the transaction to include the amount and unlocking script in each transaction input.

So now, we have a complete transaction that can be fully validated. However, we do not know if the information in the transaction is correct.  We need to check that the transaction is valid and that the previous transaction outputs have not been spent.  This is where the SUTOS comes in.  SUTOS is a key-value store where the key is the hash of an input, the value is the transaction that is spending it.


The key is calculated by sha256 hashing all 4 elements of the UTXO in the following order and format:

Table 1: Key format

| Element | Length |
| --- | --- |
| Transaction hash | 32 bytes (little-endian) |
| Output index | 4 bytes |
| Unlocking script Variable bytes |
| Amount | 8 bytes (little-endian) |
