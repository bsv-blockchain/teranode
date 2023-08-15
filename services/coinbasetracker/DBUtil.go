package coinbasetracker

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/model"
	sqlerr "github.com/mattn/go-sqlite3"
	"github.com/ordishs/go-utils"
	"gorm.io/gorm"
)

const DB_OP_RETRIES = 10
const DB_OP_WAIT = 250

func unlockSpendable(dbm base.DbManager, log utils.Logger) error {
	//const stmt = "WITH RECURSIVE ChainBlocks AS (SELECT block_hash, prev_block_hash, height FROM blocks WHERE height <= (SELECT MAX(height) - 100 FROM blocks) UNION ALL SELECT b.block_hash, b.prev_block_hash, b.height FROM blocks b JOIN ChainBlocks cb ON b.block_hash = cb.prev_block_hash) UPDATE utxos SET status = '1' WHERE block_hash IN (SELECT block_hash FROM ChainBlocks) AND status = '0'"
	var err error
	tx, _ := dbm.GetDB().(*gorm.DB)
	const stmt = "UPDATE utxos SET status = '1' WHERE ID IN (SELECT ID FROM blocks WHERE height <= (SELECT MAX(height) - 100 FROM blocks) AND status = '0')"

	retries := 0
	for retries < DB_OP_RETRIES {
		tx = tx.Exec(stmt, nil)
		if err == nil {
			break
		}
		e, ok := err.(sqlerr.Error)
		if ok {
			if e.ExtendedCode == 5 {
				log.Warnf("[unlock spendable] db locked: %s - code %d: Retrying...", e.Error(), e.ExtendedCode)
			} else {
				log.Errorf("%s - code %d: Retrying...", e.Error(), e.ExtendedCode)
			}
		} else {
			log.Errorf("[unlock spendable] unlocking spendable utxos: %s", tx.Error.Error())
		}
		retries++
		time.Sleep(DB_OP_WAIT * time.Millisecond)
	}
	return tx.Error
}

func LockSpent(dbm base.DbManager, log utils.Logger, utxo *model.UTXO) error {
	//const stmt = "WITH RECURSIVE ChainBlocks AS (SELECT block_hash, prev_block_hash, height FROM blocks WHERE height <= (SELECT MAX(height) - 100 FROM blocks) UNION ALL SELECT b.block_hash, b.prev_block_hash, b.height FROM blocks b JOIN ChainBlocks cb ON b.block_hash = cb.prev_block_hash) UPDATE utxos SET status = '1' WHERE block_hash IN (SELECT block_hash FROM ChainBlocks) AND status = '0'"
	var err error
	tx, _ := dbm.GetDB().(*gorm.DB)
	const stmt = "UPDATE utxos SET status = '3' WHERE txid = ? AND vout = ? AND status = '2'"
	vals := []interface{}{utxo.Txid, utxo.Vout}
	retries := 0
	for retries < DB_OP_RETRIES {
		tx = tx.Exec(stmt, vals...)
		if err == nil {
			break
		}
		e, ok := err.(sqlerr.Error)
		if ok {
			if e.ExtendedCode == 5 {
				log.Warnf("[lock spent] db locked: %s - code %d: Retrying...", e.Error(), e.ExtendedCode)
			} else {
				log.Errorf("%s - code %d: Retrying...", e.Error(), e.ExtendedCode)
			}
		} else {
			log.Errorf("[lock spent] locking spent utxo: %s", tx.Error.Error())
		}
		retries++
		time.Sleep(DB_OP_WAIT * time.Millisecond)
	}
	return tx.Error
}

func AddBlocks(dbm base.DbManager, log utils.Logger, blocks []*model.Block) error {
	retries := 0
	var err error
	var tx *gorm.DB
	for retries < DB_OP_RETRIES {
		i, _ := dbm.TxBegin()
		tx, _ = i.(*gorm.DB)

		tx = tx.Create(blocks)
		if tx.Error != nil {
			retries++
			e, ok := tx.Error.(sqlerr.Error)
			if ok {
				tx.Rollback()
				switch e.ExtendedCode {
				case 1555:
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
					return err
				}
				if e.ExtendedCode == 5 {
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
				}
			} else {
				tx.Rollback()
				log.Errorf("Error adding block: %s", err.Error())
			}
			time.Sleep(DB_OP_WAIT * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return err
}
func AddBlock(dbm base.DbManager, log utils.Logger, block *model.Block) error {
	retries := 0
	var err error
	var tx *gorm.DB
	for retries < DB_OP_RETRIES {
		i, _ := dbm.TxBegin()
		tx, _ = i.(*gorm.DB)

		tx = tx.Create(block)
		if tx.Error != nil {
			retries++
			e, ok := tx.Error.(sqlerr.Error)
			if ok {
				tx.Rollback()
				switch e.ExtendedCode {
				case 1555:
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
					return err
				}
				if e.ExtendedCode == 5 {
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
					time.Sleep(DB_OP_WAIT * time.Millisecond)
				}
			} else {
				tx.Rollback()
				log.Errorf("Error adding block: %s", err.Error())
			}
			continue
		}
		break
	}
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
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
	retries := 0
	for retries < 10 {
		tx = tx.Create(utxo)
		if tx.Error != nil {
			e, ok := tx.Error.(sqlerr.Error)
			if ok {
				if e.ExtendedCode == 1555 {
					// constraint violation condition - block is already in the store
					log.Warnf("\U000026A0  utxo record %s already exists: %s - code %d", hex.EncodeToString([]byte(utxo.Txid)), e.Error(), e.ExtendedCode)
					break
				}
			} else {
				log.Errorf("error creating a utxo record %s: %s", hex.EncodeToString([]byte(utxo.Txid)), tx.Error.Error())
			}
			temp := &model.UTXO{}
			tx = tx.Model(&model.UTXO{}).Where("block_hash = ?", []interface{}{utxo.BlockHash}).Find(temp)
			if tx.Error == nil {
				if temp.BlockHash == utxo.BlockHash {
					break
				}
			}
			tx.Rollback()
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}

	if tx.Error != nil {
		tx.Rollback()
	} else {
		tx.Commit()
		err := unlockSpendable(dbm, log)
		if err != nil {
			log.Errorf("Error in AddBlock to unlock spendables: %s", err.Error())
		}
	}

	return tx.Error
}

func GetBestBlockFromDb(dbm base.DbManager, log utils.Logger) (*model.Block, error) {
	m := &model.Block{}
	stmt := "SELECT ID, height, block_hash, prev_block_hash FROM blocks WHERE height = (SELECT max(height) FROM blocks)"
	db, ok := dbm.GetDB().(*gorm.DB)
	if !ok {
		panic("not a gorm.DB object")
	}
	rows, err := db.Raw(stmt, nil).Rows()
	if err != nil {
		e, ok := err.(sqlerr.Error)
		if ok {
			log.Errorf("error getting best block: %s - code %d", e.Error(), e.ExtendedCode)
		} else {
			log.Errorf("error getting best block: %s", e.Error())
		}
		return nil, err
	}

	if rows.Next() {
		err = rows.Scan(&m.ID, &m.Height, &m.BlockHash, &m.PrevBlockHash)
		if err != nil {
			log.Errorf("failed to scan: %v", err)
			return nil, err
		}
	} else {
		return nil, errors.New("no blocks found")
	}

	return m, err
}

func AddBlockUtxos(dbm base.DbManager, log utils.Logger, blocks []*model.Block, utxos []*model.UTXO) error {
	retries := 0
	var err error
	var tx *gorm.DB
	i, _ := dbm.TxBegin()
	tx, _ = i.(*gorm.DB)
	sp1 := fmt.Sprintf("add_blocks_%d", time.Now().Nanosecond())
	tx = tx.SavePoint(sp1)
	for retries < DB_OP_RETRIES {
		tx = tx.Create(blocks)
		if tx.Error != nil {
			retries++
			e, ok := tx.Error.(sqlerr.Error)
			if ok {
				tx.RollbackTo(sp1)
				if e.ExtendedCode == 1555 {
					log.Warnf("duplicate block is being inserted. %s - code %d", e.Error(), e.ExtendedCode)
					return err
				}
				if e.ExtendedCode == 5 {
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
				}
			} else {
				tx.RollbackTo(sp1)
				log.Errorf("Error adding block: %s", err.Error())
			}
			time.Sleep(DB_OP_WAIT * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		log.Errorf("Error adding block: %s", err.Error())
		tx.Rollback()
		return err
	}
	tx.Commit()

	i, _ = dbm.TxBegin()
	tx, _ = i.(*gorm.DB)

	sp2 := fmt.Sprintf("add_utxos_%d", time.Now().Nanosecond())
	tx = tx.SavePoint(sp2)
	for retries < 100 {
		tx = tx.Create(utxos)
		if tx.Error != nil {
			retries++
			e, ok := tx.Error.(sqlerr.Error)
			if ok {
				tx.RollbackTo(sp2)
				switch e.ExtendedCode {
				case 1555:
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
					return err
				}
				if e.ExtendedCode == 5 {
					log.Warnf("error adding block. %s - code %d", e.Error(), e.ExtendedCode)
					time.Sleep(250 * time.Millisecond)
				}
			} else {
				tx.RollbackTo(sp2)
				log.Errorf("Error adding block: %s", err.Error())
			}
			continue
		}
		break
	}
	if err != nil {
		log.Errorf("Error adding utxos: %s", err.Error())
		tx.Rollback()
		return err
	}

	tx.Commit()

	err = unlockSpendable(dbm, log)
	if err != nil {
		log.Errorf("failed to unlock spendable: %s", err.Error())
	}

	return err
}
