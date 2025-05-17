package plan

import (
	"fmt"

	"github.com/CefBoud/CefDB/metadata"
	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type BasicUpdatePlanner struct {
	Md *metadata.MetadataMgr
}

func NewBasicUpdatePlanner(md *metadata.MetadataMgr) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{Md: md}
}

func (bup *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) (int, error) {
	l, err := bup.Md.GetLayout(data.Table, tx)
	if err != nil {
		return 0, fmt.Errorf("ExecuteInsert GetLayout error: %v", err)
	}
	ts, err := record.NewTableScan(tx, data.Table, l)
	if err != nil {
		return 0, fmt.Errorf("ExecuteInsert NewTableScan error: %v", err)
	}
	ts.Insert()
	for i, f := range data.Fields {
		ts.SetVal(f, data.Values[i])
	}
	ts.Close()

	return 1, nil
}

func (bup *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) (int, error) {
	var affectedRows int

	tp, err := NewTablePlan(data.Table, tx, bup.Md)
	if err != nil {
		return 0, fmt.Errorf("ExecuteDelete NewTablePlan error: %v", err)
	}
	// ts, err := .NewTableScan(tx, data.Table, l)
	sp := NewSelectPlan(tp, data.Predicate)
	ss, err := sp.Open()
	us := ss.(query.UpdateScan)
	if err != nil {
		return 0, fmt.Errorf("ExecuteDelete SelectPlan.Open() error: %v", err)
	}
	for us.Next() {
		us.Delete()
		affectedRows++
	}
	us.Close()
	return affectedRows, nil
}

func (bup *BasicUpdatePlanner) ExecuteModify(data *parser.UpdateData, tx *tx.Transaction) (int, error) {
	var affectedRows int

	tp, err := NewTablePlan(data.Table, tx, bup.Md)
	if err != nil {
		return 0, fmt.Errorf("ExecuteModify NewTablePlan error: %v", err)
	}
	sp := NewSelectPlan(tp, data.Predicate)
	ss, err := sp.Open()
	us := ss.(query.UpdateScan)
	if err != nil {
		return 0, fmt.Errorf("ExecuteModify SelectPlan.Open() error: %v", err)
	}
	for us.Next() {
		exprValue, err := data.Expression.Evaluate(us)
		if err != nil {
			return 0, fmt.Errorf("ExecuteModify Expression.Evaluate error: %v", err)
		}
		us.SetVal(data.Field, exprValue)
		affectedRows++
	}
	us.Close()
	return affectedRows, nil
}
func (bup *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) (int, error) {
	return 0, bup.Md.CreateTable(data.Table, data.Schema, tx)
}

// func (bup *BasicQueryPlan) ExecuteCreateIndex(data *parser.CreateTableData, tx *tx.Transaction) (int, error) {
// 	return 0, bup.Md.CreateTable(data.Table, data.Schema, tx)
// }
