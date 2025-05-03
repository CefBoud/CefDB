package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type CommitRecord struct {
	txNum int
}

func NewCommitRecord(b []byte) *CommitRecord {
	p := file.NewPageFromBytes(b)
	return &CommitRecord{
		txNum: p.GetInt(4),
	}
}
func (r *CommitRecord) String() string {
	return fmt.Sprintf(
		"LogRecord{TxNum: %v, Op: COMMIT}",
		r.txNum,
	)
}

func (cr *CommitRecord) Op() int {
	return COMMIT
}

func (cr *CommitRecord) TxNumber() int {
	return cr.txNum
}

func (cr *CommitRecord) Undo(tx Transaction) {}

// WriteCommitRecordToLog appends a commit record to the log and return the LSN and error
func WriteCommitRecordToLog(lm *log.LogMgr, txnum int) (int, error) {
	b := make([]byte, 8)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, COMMIT)
	p.SetInt(4, txnum)
	return lm.Append(p.Contents())
}
