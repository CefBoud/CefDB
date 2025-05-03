package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type RollbackRecord struct {
	txNum int
}

func NewRollbackRecord(b []byte) *RollbackRecord {
	p := file.NewPageFromBytes(b)
	return &RollbackRecord{
		txNum: p.GetInt(4),
	}
}

func (r *RollbackRecord) String() string {
	return fmt.Sprintf(
		"LogRecord{TxNum: %v, Op: ROLLBACK}",
		r.txNum,
	)
}

func (rr *RollbackRecord) Op() int {
	return ROLLBACK
}

func (rr *RollbackRecord) TxNumber() int {
	return rr.txNum
}

func (rr *RollbackRecord) Undo(tx Transaction) {}

// WriteRollbackRecordToLog appends a ROLLBACK record to the log and return the LSN and error
func WriteRollbackRecordToLog(lm *log.LogMgr, txnum int) (int, error) {
	b := make([]byte, 8)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, ROLLBACK)
	p.SetInt(4, txnum)
	return lm.Append(p.Contents())
}
