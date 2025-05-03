package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type SetIntRecord struct {
	txNum  int
	blk    *file.BlockId
	offset int
	oldVal int
	newVal int
}

func NewSetIntRecord(b []byte) *SetIntRecord {
	p := file.NewPageFromBytes(b)
	txNum := p.GetInt(4)
	fileName := p.GetString(8)
	blknum := p.GetInt(12 + len(fileName))
	offset := p.GetInt(16 + len(fileName))
	oldVal := p.GetInt(20 + len(fileName))
	newVal := p.GetInt(24 + len(fileName))
	return &SetIntRecord{
		txNum:  txNum,
		blk:    &file.BlockId{Filename: fileName, Blknum: blknum},
		offset: offset,
		oldVal: oldVal,
		newVal: newVal,
	}
}

func (r *SetIntRecord) String() string {
	return fmt.Sprintf(
		"LogRecord{TxNum: %v, Op: SETINT, FileName: %v, Blknum: %v, Offset: %v, OldVal: %v, NewVal: %v}",
		r.txNum,
		r.blk.Filename,
		r.blk.Blknum,
		r.offset,
		r.oldVal,
		r.newVal,
	)
}

func (r *SetIntRecord) Op() int {
	return SETINT
}

func (r *SetIntRecord) TxNumber() int {
	return r.txNum
}

func (r *SetIntRecord) Undo(tx Transaction) {
	tx.Pin(r.blk)
	tx.SetInt(r.blk, r.offset, r.oldVal, false) // do not log Undo :)
	tx.Unpin(r.blk)
}

// WriteSetIntRecordToLog appends a setint record to the log and return the LSN and error
func WriteSetIntRecordToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, oldVal int, newVal int) (int, error) {
	b := make([]byte, len(blk.Filename)+28)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, SETINT)
	p.SetInt(4, txnum)
	p.SetString(8, blk.Filename)
	p.SetInt(12+len(blk.Filename), blk.Blknum)
	pos := 16 + len(blk.Filename)
	p.SetInt(pos, offset)
	p.SetInt(pos+4, oldVal)
	p.SetInt(pos+8, newVal)
	return lm.Append(p.Contents())
}
