package tx

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

type SetStringRecord struct {
	txNum  int
	blk    *file.BlockId
	offset int
	oldVal string
	newVal string
}

func NewSetStringRecord(b []byte) *SetStringRecord {
	p := file.NewPageFromBytes(b)
	txNum := p.GetInt(4)
	fileName := p.GetString(8)
	blknum := p.GetInt(12 + len(fileName))
	offset := p.GetInt(16 + len(fileName))
	oldVal := p.GetString(20 + len(fileName))
	newVal := p.GetString(24 + len(fileName) + len(oldVal))
	return &SetStringRecord{
		txNum:  txNum,
		blk:    &file.BlockId{Filename: fileName, Blknum: blknum},
		offset: offset,
		oldVal: oldVal,
		newVal: newVal,
	}
}

func (r *SetStringRecord) String() string {
	return fmt.Sprintf(
		"LogRecord{TxNum: %v, Op: SETSTRING, FileName: %v, Blknum: %v, Offset: %v, OldVal: %v, NewVal: %v}",
		r.txNum,
		r.blk.Filename,
		r.blk.Blknum,
		r.offset,
		r.oldVal,
		r.newVal,
	)
}

func (r *SetStringRecord) Op() int {
	return SETSTRING
}

func (r *SetStringRecord) TxNumber() int {
	return r.txNum
}

func (r *SetStringRecord) Undo(tx Transaction) {
	tx.Pin(r.blk)
	tx.SetString(r.blk, r.offset, r.oldVal, false) // do not log Undo :)
	tx.Unpin(r.blk)
}

// WriteSetStringRecordToLog appends a setstring record to the log and return the LSN and error
func WriteSetStringRecordToLog(lm *log.LogMgr, txnum int, blk *file.BlockId, offset int, oldVal string, newVal string) (int, error) {
	b := make([]byte, len(blk.Filename)+len(oldVal)+len(newVal)+28)
	p := file.NewPageFromBytes(b)
	p.SetInt(0, SETSTRING)
	p.SetInt(4, txnum)
	p.SetString(8, blk.Filename)
	p.SetInt(12+len(blk.Filename), blk.Blknum)

	p.SetInt(16+len(blk.Filename), offset)
	p.SetString(20+len(blk.Filename), oldVal)
	p.SetString(24+len(blk.Filename)+len(oldVal), newVal)
	return lm.Append(p.Contents())
}
