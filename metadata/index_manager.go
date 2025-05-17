package metadata

import (
	"fmt"

	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type IndexMgr struct {
	tableMgr *TableMgr
	layout   *record.Layout
}

const IndexCatalogName = "idxcat"

func NewIndexMgr(isNew bool, tm *TableMgr, tx *tx.Transaction) (*IndexMgr, error) {
	var err error
	if isNew {
		indexCatalogSchema := record.NewSchema()
		indexCatalogSchema.AddStringField("indexname", MAX_NAME)
		indexCatalogSchema.AddStringField("tablename", MAX_NAME)
		indexCatalogSchema.AddStringField("fieldname", MAX_NAME)
		err = tm.CreateTable(IndexCatalogName, indexCatalogSchema, tx)
		if err != nil {
			return nil, fmt.Errorf("NewIndexMgr error: %v", err)
		}
	}
	l, _ := tm.GetLayout(IndexCatalogName, tx)
	return &IndexMgr{layout: l, tableMgr: tm}, nil
}

func (im *IndexMgr) CreateIndex(indexname, tablename, fieldname string, tx *tx.Transaction) error {
	ts, err := record.NewTableScan(tx, tablename, im.layout)
	if err != nil {
		return fmt.Errorf("CreateIndex error: %v", err)
	}
	ts.Insert()
	ts.SetString("indexname", indexname)
	ts.SetString("tablename", tablename)
	ts.SetString("fieldname", fieldname)
	ts.Close()

	return nil
}
