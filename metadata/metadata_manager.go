package metadata

import (
	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type MetadataMgr struct {
	tableMgr *TableMgr
	indexMgr *IndexMgr
	statMgr  *StatMgr
}

func NewMetadataMgr(isNew bool, tx *tx.Transaction) *MetadataMgr {
	tm := NewTableMgr(isNew, tx)
	im, err := NewIndexMgr(isNew, tm, tx)
	if err != nil {
		panic("NewMetadataMgr error: " + err.Error())
	}

	sm := NewStatManager(tm, tx)
	return &MetadataMgr{tableMgr: tm, indexMgr: im, statMgr: sm}
}

func (mm *MetadataMgr) CreateTable(tblname string, sch *record.Schema, tx *tx.Transaction) error {
	return mm.tableMgr.CreateTable(tblname, sch, tx)
}
func (mm *MetadataMgr) GetLayout(tblname string, tx *tx.Transaction) (*record.Layout, error) {
	return mm.tableMgr.GetLayout(tblname, tx)
}

func (mm *MetadataMgr) CreateIndex(indexname, tablename, fieldname string, tx *tx.Transaction) error {
	return mm.indexMgr.CreateIndex(indexname, tablename, fieldname, tx)
}

func (mm *MetadataMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	return mm.statMgr.GetStatInfo(tblname, layout, tx)
}
