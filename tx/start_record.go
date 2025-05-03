package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type StartRecord struct {
	txNum int
}

func NewStartRecord(b []byte) *StartRecord {
	p := file.NewPageFromBytes(b)
	return &StartRecord{
		txNum: p.GetInt(4),
	}
}

func (r *StartRecord) String() string {
	return fmt.Sprintf(
		"LogRecord{TxNum: %v, Op: START}",
		r.txNum,
	)
}

func (sr *StartRecord) Op() int {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txNum
}

func (sr *StartRecord) Undo(tx Transaction) {}

// WriteStartRecordToLog appends a start record to the log and return the LSN and error
func WriteStartRecordToLog(lm *log.LogMgr, txnum int) (int, error) {
	b := make([]byte, 8)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, START)
	p.SetInt(4, txnum)
	return lm.Append(p.Contents())
}
