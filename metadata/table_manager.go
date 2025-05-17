package metadata

import (
	"fmt"

	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type TableMgr struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

const MAX_NAME = 16
const TableCatalogName = "tblcat"
const FieldCatalogName = "fldcat"

func NewTableMgr(isNew bool, tx *tx.Transaction) *TableMgr {
	tm := &TableMgr{}
	tableCatalogSchema := record.NewSchema()
	tableCatalogSchema.AddStringField("tblname", MAX_NAME)
	tableCatalogSchema.AddIntField("slotsize")
	tm.tableCatalogLayout = record.NewLayout(tableCatalogSchema)

	fieldCatalogSchema := record.NewSchema()
	fieldCatalogSchema.AddStringField("tblname", MAX_NAME)
	fieldCatalogSchema.AddStringField("fldname", MAX_NAME)
	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")
	tm.fieldCatalogLayout = record.NewLayout(fieldCatalogSchema)

	if isNew {
		tm.CreateTable(TableCatalogName, tableCatalogSchema, tx)
		tm.CreateTable(FieldCatalogName, fieldCatalogSchema, tx)
	}
	return tm
}

func (tm *TableMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) error {
	l := record.NewLayout(sch)

	ts, err := record.NewTableScan(tx, TableCatalogName, tm.tableCatalogLayout)
	if err != nil {
		return fmt.Errorf("Error CreateTable '%v' : %v", tblname, err)
	}
	ts.Insert()
	ts.SetString("tblname", tblname)
	ts.SetInt("slotsize", l.SlotSize)
	ts.Close()

	ts, err = record.NewTableScan(tx, FieldCatalogName, tm.fieldCatalogLayout)
	if err != nil {
		return fmt.Errorf("Error CreateTable '%v' : %v", tblname, err)
	}
	for field, fieldInfo := range l.Schema.Fields {
		ts.Insert()
		ts.SetString("tblname", tblname)
		ts.SetString("fldname", field)
		ts.SetInt("type", fieldInfo.Type)
		ts.SetInt("length", fieldInfo.Length)
		ts.SetInt("offset", l.Offset(field))
	}
	ts.Close()
	return nil
}

func (tm *TableMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	fieldTableScan, err := record.NewTableScan(tx, FieldCatalogName, tm.fieldCatalogLayout)
	sch := record.NewSchema()
	if err != nil {
		return nil, fmt.Errorf("error GetLayout '%v' : %v", tblname, err)
	}
	for fieldTableScan.Next() {
		t, err := fieldTableScan.GetString("tblname")
		if err != nil {
			return nil, fmt.Errorf("Error GetLayout '%v' : %v", tblname, err)
		}
		if t == tblname {
			fname, _ := fieldTableScan.GetString("fldname")
			ftype, _ := fieldTableScan.GetInt("type")
			flength, _ := fieldTableScan.GetInt("length")
			sch.AddField(fname, ftype, flength)
		}
	}
	return record.NewLayout(sch), nil
}
