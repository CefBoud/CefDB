package plan

import (
	"fmt"

	"github.com/CefBoud/CefDB/metadata"
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type TablePlan struct {
	TableName string
	Tx        *tx.Transaction
	Layout    *record.Layout
	StatInfo  metadata.StatInfo
}

func NewTablePlan(tlbname string, tx *tx.Transaction, md *metadata.MetadataMgr) (*TablePlan, error) {
	l, err := md.GetLayout(tlbname, tx)
	if err != nil {
		return nil, fmt.Errorf("NewTablePlan error : %v", err)
	}
	si := md.GetStatInfo(tlbname, l, tx)
	return &TablePlan{TableName: tlbname, Tx: tx, Layout: l, StatInfo: si}, nil
}

func (tp *TablePlan) Open() (query.Scan, error) {
	return record.NewTableScan(tp.Tx, tp.TableName, tp.Layout)
}
func (tp *TablePlan) BlocksAccessed() int {
	return tp.StatInfo.BlocksAccessed()
}
func (tp *TablePlan) RecordsOutput() int {
	return tp.StatInfo.RecordsOutput()
}
func (tp *TablePlan) DistinctValues(fldname string) int {
	return tp.StatInfo.DistinctValues(fldname)
}
func (tp *TablePlan) Schema() *record.Schema {
	return tp.Layout.Schema
}
