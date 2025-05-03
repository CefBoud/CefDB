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

func TestRecordPage(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestRecordPage")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 128)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"
	testFileName := "testfiletx"
	for i := 0; i < 10; i++ {
		fm.Append(testFileName)
	}

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)
	blk := file.NewBlockId(testFileName, 1)

	// Transaction 1: Write data without logging
	tx1 := tx.NewTransaction(fm, lm, bm)
	assert.NoError(t, tx1.Pin(blk))

	s := NewSchema()
	s.AddIntField("A")
	s.AddStringField("B", 10)
	l := NewLayout(s)

	rp, _ := NewRecordPage(tx1, blk, l)

	slot, _ := rp.InsertAfter(-1)

	var i int
	for slot > -1 {
		// fmt.Printf("InsertAfter slot %v\n", slot)
		rp.SetInt(slot, "A", i)
		rp.SetString(slot, "B", fmt.Sprintf("record%v", i))
		slot, _ = rp.InsertAfter(slot)
		i++
	}

	slot = rp.NextAfter(-1)
	for slot > -1 {
		i, _ = rp.GetInt(slot, "A")
		if i%2 == 0 {
			rp.Delete(slot)
		}
		slot = rp.NextAfter(slot)
	}

	tx1.UnpinAll()
	tx1.Commit()

	tx2 := tx.NewTransaction(fm, lm, bm)
	assert.NoError(t, tx2.Pin(blk))

	rp, _ = NewRecordPage(tx2, blk, l)

	slot = rp.NextAfter(-1)
	var actuals_A []int
	var actuals_B []string
	for slot > -1 {
		// fmt.Printf("Get slot %v\n", slot)
		i, _ = rp.GetInt(slot, "A")
		actuals_A = append(actuals_A, i)
		s, _ := rp.GetString(slot, "B")
		actuals_B = append(actuals_B, s)
		slot = rp.NextAfter(slot)
	}
	assert.Equal(t, []int{1, 3}, actuals_A)
	assert.Equal(t, []string{"record1", "record3"}, actuals_B)

}
