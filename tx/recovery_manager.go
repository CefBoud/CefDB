package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/log"
)

type RecoveryMgr struct {
	tx *Transaction
	bm *buffer.BufferMgr
	// fm *file.FileMgr
	lm *log.LogMgr
}

func NewRecoveryMgr(tx *Transaction, lm *log.LogMgr, bm *buffer.BufferMgr) *RecoveryMgr {
	WriteStartRecordToLog(lm, tx.txnum)
	return &RecoveryMgr{
		tx: tx,
		lm: lm,
		bm: bm,
	}
}

func (rm *RecoveryMgr) Commit() error {
	rm.bm.FlushAll(rm.tx.txnum)
	lsn, err := WriteCommitRecordToLog(rm.lm, rm.tx.txnum)
	if err != nil {
		return fmt.Errorf("Error WriteCommitRecordToLog tx[%v]: %v ", rm.tx.txnum, err)
	} else {
		rm.lm.Flush(lsn)
	}
	return nil
}

func (rm *RecoveryMgr) Rollback() error {
	iter, err := rm.lm.Iterator()
	if err != nil {
		return fmt.Errorf("Error getting log iterator while running Rollback for tx[%v]: %v ", rm.tx.txnum, err)
	}
	for {
		bytes := iter.NextRecord()
		if bytes == nil {
			break
		}
		r := CreateLogRecord(bytes)
		if r.TxNumber() == rm.tx.txnum {
			if r.Op() == START {
				break
			}
			r.Undo(*rm.tx)
		}
	}
	lsn, err := WriteRollbackRecordToLog(rm.lm, rm.tx.txnum)
	if err != nil {
		return fmt.Errorf("Error WriteRollbackRecordToLog tx[%v]: %v ", rm.tx.txnum, err)
	} else {
		rm.lm.Flush(lsn)
	}
	return nil
}

func (rm *RecoveryMgr) SetInt(buff *buffer.Buffer, offset int, val int) (int, error) {
	old := buff.Contents().GetInt(offset)
	return WriteSetIntRecordToLog(rm.lm, rm.tx.txnum, buff.Block(), offset, old, val)
}

func (rm *RecoveryMgr) SetString(buff *buffer.Buffer, offset int, val string) (int, error) {
	old := buff.Contents().GetString(offset)
	return WriteSetStringRecordToLog(rm.lm, rm.tx.txnum, buff.Block(), offset, old, val)
}

// Recover uncompleted transactions from the log
// and then write a quiescent checkpoint record to the log and flush it.

func (rm *RecoveryMgr) Recover() error {
	iter, err := rm.lm.Iterator()
	if err != nil {
		return fmt.Errorf("Error getting log iterator while running Recover for: %v ", err)
	}
	finishedTransactions := make(map[int]bool)
	for {
		// The loop stops when it encounters a CHECKPOINT record
		bytes := iter.NextRecord()
		if bytes == nil {
			break
		}
		r := CreateLogRecord(bytes)
		if r.Op() == CHECKPOINT {
			break
		} else if r.Op() == COMMIT || r.Op() == ROLLBACK {
			finishedTransactions[r.TxNumber()] = true
		} else if !finishedTransactions[r.TxNumber()] { // not finished
			r.Undo(*rm.tx)
		}
	}

	// once we revert all unfinished tx, we flush buffers to disk and write CHECKPOINT log record
	rm.bm.FlushAll(rm.tx.txnum)
	lsn, err := WriteCheckpointRecordToLog(rm.lm)
	if err != nil {
		return fmt.Errorf("Error WriteCheckpointRecordToLog tx[%v]: %v ", rm.tx.txnum, err)
	} else {
		rm.lm.Flush(lsn)
	}
	return nil
}
