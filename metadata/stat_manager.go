package metadata

import (
	"fmt"
	"sync"

	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
)

type StatMgr struct {
	TableManager *TableMgr
	TableStats   map[string]StatInfo
	NumCalls     int // refresh every 100 calls
	sync.Mutex
}

func NewStatManager(tm *TableMgr, tx *tx.Transaction) *StatMgr {
	sm := &StatMgr{
		TableManager: tm,
		TableStats:   make(map[string]StatInfo),
	}
	sm.refreshStatistics(tx)
	return sm
}

func (sm *StatMgr) GetStatInfo(tblname string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	sm.Lock()
	defer sm.Unlock()

	sm.NumCalls++
	if sm.NumCalls > 100 {
		err := sm.refreshStatistics(tx)
		if err != nil {
			fmt.Printf("error refreshStatistics: %v\n", err)
		}
	}

	if si, ok := sm.TableStats[tblname]; ok {
		return si
	}

	si := sm.calcTableStats(tblname, layout, tx)
	sm.TableStats[tblname] = si
	return si
}

func (sm *StatMgr) refreshStatistics(tx *tx.Transaction) error {
	sm.TableStats = make(map[string]StatInfo)
	sm.NumCalls = 0

	tcatLayout, _ := sm.TableManager.GetLayout(TableCatalogName, tx)
	tcat, err := record.NewTableScan(tx, TableCatalogName, tcatLayout)
	defer tcat.Close()
	if err != nil {
		return fmt.Errorf("error refreshStatistics: %v", err)
	}
	for tcat.Next() {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return fmt.Errorf("error refreshStatistics: %v", err)
		}
		layout, err := sm.TableManager.GetLayout(tblname, tx)
		if err != nil {
			return fmt.Errorf("error refreshStatistics: %v", err)
		}
		si := sm.calcTableStats(tblname, layout, tx)
		sm.TableStats[tblname] = si
	}
	return nil
}

func (sm *StatMgr) calcTableStats(tblname string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	numRecs := 0
	numBlocks := 0

	ts, _ := record.NewTableScan(tx, tblname, layout)
	defer ts.Close()

	for ts.Next() {
		numRecs++
		rid := ts.GetRid()
		if rid.BlkNum+1 > numBlocks {
			numBlocks = rid.BlkNum + 1
		}
	}

	return StatInfo{
		NumBlocks: numBlocks,
		NumRecs:   numRecs,
	}
}
