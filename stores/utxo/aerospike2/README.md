
Normal TX
---------
inputs
outputs
version
locktime
fee
sizeInBytes
utxos - this is the list of utxos in the tx
nrUtxos - this is total number of utxos in the tx
spentUtxos - this is number of utxos that are spent in the tx
blockIDs
isCoinbase
spendingHeight
frozen


BIG TX with < 20,000 outputs
----------------------------
big = true
fee
sizeInBytes
utxos - this is the list of utxos in the tx
nrUtxos - this is total number of utxos in the tx
spentUtxos - this is number of utxos that are spent in the tx
blockIDs
isCoinbase
spendingHeight
frozen


TX with > 20,000 outputs (split into multiple records)
------------------------------------------------------

Record 1 - Key of TXID (the _0 is implied)

big = true
fee
sizeInBytes
utxos - utxo[0..19999]
nrUtxos - this is number of utxos in this record
spentUtxos - this is number of utxos that are spent in the tx
blockIDs
isCoinbase
spendingHeight
frozen


Record 2 - Key of TXID_1

utxos - utxo[20000..39999]
nrUtxos - this is number of utxos in this record
spentUtxos - this is number of utxos that are spent in the tx
isCoinbase
spendingHeight
frozen


When I want to spend a utxo with a vout of 25000, the formula to calculate the record number is:

key = txid

recordNumber = floor(vout/20000)
if recordNumber > 0 then
    key = key + "_" + recordNumber
end

```
