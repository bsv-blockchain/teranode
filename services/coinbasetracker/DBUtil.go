package coinbasetracker

import (
	"database/sql"
	"encoding/hex"
	"errors"

	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/model"
	networkModel "github.com/TAAL-GmbH/ubsv/model"
	sqlerr "github.com/mattn/go-sqlite3"
	"github.com/ordishs/go-utils"
	"gorm.io/gorm"
)

func unlockSpendable(dbm base.DbManager, log utils.Logger) error {
	const stmt = `
			WITH RECURSIVE ChainBlocks AS (	SELECT block_hash, prev_block_hash, height FROM blocks WHERE height <= (SELECT MAX(height) - 100 FROM blocks) UNION ALL SELECT b.block_hash, b.prev_block_hash, b.height FROM blocks b JOIN ChainBlocks cb ON b.block_hash = cb.prev_block_hash) UPDATE utxos SET status = '1'	WHERE block_hash IN (SELECT block_hash FROM ChainBlocks) 	AND status = '0';  
	`
	var err error
	i := dbm.GetDB()
	dbe, ok := i.(*gorm.DB)
	if !ok {
		err = errors.New("db is not a gorm database object")
		log.Errorf("%s", err.Error())
		return err
	}
	tx := dbe.Exec(stmt)
	if tx.Error != nil {
		log.Errorf("%s", tx.Error)
	}
	return tx.Error
}

func AddBlock(dbm base.DbManager, log utils.Logger, block *model.Block) error {
	err := dbm.Create(block)
	if err != nil {
		log.Errorf("Error adding block: %s", err.Error())
		return err
	}
	err = unlockSpendable(dbm, log)
	if err != nil {
		log.Errorf("Error in AddBlock to unlock spendables: %s", err.Error())
	}
	return err
}
func AddUtxo(dbm base.DbManager, log utils.Logger, utxo *model.UTXO) error {
	txopts := []*sql.TxOptions{{Isolation: sql.LevelSerializable, ReadOnly: false}}
	i, _ := dbm.TxBegin(txopts...)
	tx, ok := i.(*gorm.DB)
	if !ok {
		// something is critically wrong
		panic("could not cast to gorm.DB")
	}
	defer tx.Commit()
	var res *gorm.DB
	retries := 0
	for retries < 10 {
		res = tx.Create(utxo)
		if res.Error != nil {
			e, ok := res.Error.(sqlerr.Error)
			if ok {
				if e.ExtendedCode == 1555 {
					// constraint violation condition - block is already in the store
					log.Warnf("\U000026A0  utxo record %s already exists: %s - code %d", hex.EncodeToString([]byte(utxo.Txid)), e.Error(), e.ExtendedCode)
					break
				}
			} else {
				log.Errorf("error creating a utxo record %s: %s", hex.EncodeToString([]byte(utxo.Txid)), res.Error.Error())
			}
			temp := &model.UTXO{}
			tx = tx.Model(&model.UTXO{}).Where("block_hash = ?", []interface{}{utxo.BlockHash}).Find(temp)
			if tx.Error == nil {
				if temp.BlockHash == utxo.BlockHash {
					break
				}
			}
			res.Rollback()
		}
	}
	return res.Error
}

func GetBestBlockFromDb(dbm base.DbManager, log utils.Logger) (*model.Block, error) {
	m := &model.Block{}
	err := dbm.Read(m) // TODO: change logic to retrieve truly best block
	if err != nil {
		log.Errorf(err.Error())
	}
	return m, err
}

func SaveCoinbaseUtxos(dbm base.DbManager, log utils.Logger, newBlock *networkModel.Block) {
	for i, o := range newBlock.CoinbaseTx.Outputs {
		addr, err := o.LockingScript.Addresses()
		if err != nil {
			log.Errorf("could not get address from script %s", err.Error())
			break
		}
		err = AddUtxo(dbm, log,
			&model.UTXO{
				BlockHash:     newBlock.Hash().String(),
				Txid:          newBlock.CoinbaseTx.TxID(),
				Vout:          uint32(i),
				LockingScript: o.LockingScript.String(),
				Satoshis:      o.Satoshis,
				Address:       addr[0],
			})
		if err != nil {
			log.Errorf("could not add utxo to db %+v", err)
			break
		}
	}
}
