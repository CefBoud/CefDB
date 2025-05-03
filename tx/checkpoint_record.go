package tx

import (
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type CheckpointRecord struct{}

func NewCheckpointRecord(b []byte) *CheckpointRecord {
	return &CheckpointRecord{}
}

func (r *CheckpointRecord) String() string {
	return "LogRecord{Op: CHECKPOINT}"
}

func (cr *CheckpointRecord) Op() int {
	return CHECKPOINT
}

func (cr *CheckpointRecord) TxNumber() int {
	return -1
}

func (cr *CheckpointRecord) Undo(tx Transaction) {}

// WriteCheckpointRecordToLog appends a CHECKPOINT record to the log and return the LSN and error
func WriteCheckpointRecordToLog(lm *log.LogMgr) (int, error) {
	b := make([]byte, 4)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, CHECKPOINT)
	return lm.Append(p.Contents())
}
