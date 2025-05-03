package record

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/CefBoud/CefDB/tx"
	"github.com/stretchr/testify/assert"
)

func TestTableScan(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestTableScan")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 70)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"
	testFileName := "testfiletx"
	for i := 0; i < 10; i++ {
		fm.Append(testFileName)
	}

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)

	// Transaction 1: Write data without logging
	tx1 := tx.NewTransaction(fm, lm, bm)

	s := NewSchema()
	s.AddIntField("A")
	s.AddStringField("B", 20)
	l := NewLayout(s)

	ts, _ := NewTableScan(tx1, "MaTable", l) //NewRecordPage(tx1, blk, l)

	for i := 0; i < 11; i++ {
		assert.NoError(t, ts.Insert())
		// fmt.Printf("InsertAfter %v\n", i)
		ts.SetInt("A", i)
		ts.SetString("B", fmt.Sprintf("record%v", i))
	}

	ts.BeforeFirst()
	var i int
	for ts.Next() {
		i, _ = ts.GetInt("A")
		if i%2 == 0 {
			// fmt.Printf("Delete %v\n", i)
			ts.Delete()
		}
	}

	tx1.Commit()

	tx2 := tx.NewTransaction(fm, lm, bm)

	var actuals_A []int
	var actuals_B []string
	ts, _ = NewTableScan(tx2, "MaTable", l)
	for ts.Next() {
		i, _ = ts.GetInt("A")
		actuals_A = append(actuals_A, i)
		s, _ := ts.GetString("B")
		// fmt.Printf("Get  A=%v B=%v\n", i, s)
		actuals_B = append(actuals_B, s)
	}
	assert.Equal(t, []int{1, 3, 5, 7, 9}, actuals_A)
	assert.Equal(t, []string{"record1", "record3", "record5", "record7", "record9"}, actuals_B)

}
