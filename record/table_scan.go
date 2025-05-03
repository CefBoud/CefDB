package record

import (
	"fmt"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/tx"
)

// RID identifies a record within a file.
type RID struct {
	BlkNum int
	Slot   int
}

type TableScan struct {
	Tx                *tx.Transaction
	Filename          string
	Layout            *Layout
	CurrentRecordPage *RecordPage
	currentSlot       int
}

func NewTableScan(tx *tx.Transaction, tableName string, l *Layout) (*TableScan, error) {
	ts := &TableScan{Tx: tx, Filename: tableName + ".tbl", Layout: l}
	size, _ := tx.Size(ts.Filename)
	var err error
	if size == 0 {
		err = ts.MoveToNewBlock()
	} else {
		err = ts.MoveToBlock(0)
	}
	if err != nil {
		return nil, fmt.Errorf("Error creating new TableScan for '%v' : %v", tableName, err)
	}
	return ts, nil
}

func (ts *TableScan) MoveToNewBlock() error {
	blk, err := ts.Tx.Append(ts.Filename)
	if err != nil {
		return err
	}
	err = ts.MoveToBlock(blk.Blknum)
	if err != nil {
		return err
	}
	return ts.CurrentRecordPage.Format()
}

func (ts *TableScan) MoveToBlock(blknum int) error {
	ts.Unpin()
	var err error
	ts.CurrentRecordPage, err = NewRecordPage(ts.Tx, &file.BlockId{Blknum: blknum, Filename: ts.Filename}, ts.Layout)
	ts.currentSlot = -1
	return err
}

func (ts *TableScan) BeforeFirst() {
	ts.MoveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.currentSlot = ts.CurrentRecordPage.NextAfter(ts.currentSlot)
	for ts.currentSlot < 0 {
		if ts.AtLastBlock() {
			return false
		}
		ts.MoveToBlock(ts.CurrentRecordPage.Blk.Blknum + 1)
		ts.currentSlot = ts.CurrentRecordPage.NextAfter(ts.currentSlot)
	}
	return true
}

func (ts *TableScan) Insert() error {
	var err error
	ts.currentSlot, err = ts.CurrentRecordPage.InsertAfter(ts.currentSlot)
	if err != nil {
		return fmt.Errorf("TableScan Insert error: %v", err)
	}
	for ts.currentSlot < 0 {
		if ts.AtLastBlock() {
			ts.MoveToNewBlock()
		} else {
			ts.MoveToBlock(ts.CurrentRecordPage.Blk.Blknum + 1)
		}
		ts.currentSlot, err = ts.CurrentRecordPage.InsertAfter(ts.currentSlot)
		if err != nil {
			return fmt.Errorf("TableScan Insert error: %v", err)
		}

	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.CurrentRecordPage.Delete(ts.currentSlot)
}

func (ts *TableScan) AtLastBlock() bool {
	lastBlock, _ := ts.Tx.Size(ts.Filename)
	return ts.CurrentRecordPage.Blk.Blknum == lastBlock-1
}

func (ts *TableScan) GetInt(fname string) (int, error) {
	return ts.CurrentRecordPage.GetInt(ts.currentSlot, fname)
}

func (ts *TableScan) GetString(fname string) (string, error) {
	return ts.CurrentRecordPage.GetString(ts.currentSlot, fname)
}
func (ts *TableScan) GetVal(fname string, val any) (any, error) {
	if ts.Layout.Schema.FieldType(fname) == INTEGER {
		return ts.GetInt(fname)
	}
	return ts.GetString(fname)
}

func (ts *TableScan) SetInt(fname string, val int) error {
	return ts.CurrentRecordPage.SetInt(ts.currentSlot, fname, val)
}

func (ts *TableScan) SetString(fname string, val string) error {
	return ts.CurrentRecordPage.SetString(ts.currentSlot, fname, val)
}

func (ts *TableScan) SetVal(fname string, val any) error {
	if ts.Layout.Schema.FieldType(fname) == INTEGER {
		return ts.SetInt(fname, val.(int))
	}
	return ts.SetString(fname, val.(string))
}

func (ts *TableScan) MoveToRID(rid RID) error {
	err := ts.MoveToBlock(rid.BlkNum)
	if err != nil {
		return fmt.Errorf("MoveToRID error: %v", err)
	}
	ts.currentSlot = rid.Slot
	return nil
}

func (ts *TableScan) Unpin() {
	if ts.CurrentRecordPage != nil {
		ts.Tx.Unpin(ts.CurrentRecordPage.Blk)
	}
}
