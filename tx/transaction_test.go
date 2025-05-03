package tx

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/stretchr/testify/assert"
)

func TestTxBasic(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestTxBasic")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 128)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"
	testFileName := "testfiletx"
	for i := 0; i < 50; i++ {
		fm.Append(testFileName)
	}

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)
	blk := file.NewBlockId(testFileName, 1)

	// Transaction 1: Write data without logging
	tx1 := NewTransaction(fm, lm, bm)
	assert.NoError(t, tx1.Pin(blk))

	assert.NoError(t, tx1.SetInt(blk, 80, 1, false))
	assert.NoError(t, tx1.SetString(blk, 40, "one", false))
	tx1.Commit()

	// Transaction 2: Read, update with logging
	tx2 := NewTransaction(fm, lm, bm)
	assert.NoError(t, tx2.Pin(blk))

	ival, err := tx2.GetInt(blk, 80)
	assert.NoError(t, err)
	sval, err := tx2.GetString(blk, 40)
	assert.NoError(t, err)

	fmt.Printf("Initial value at location 80 = %d\n", ival)
	fmt.Printf("Initial value at location 40 = %s\n", sval)

	newIVal := ival + 1
	newSVal := sval + "!"

	assert.NoError(t, tx2.SetInt(blk, 80, newIVal, true))
	assert.NoError(t, tx2.SetString(blk, 40, newSVal, true))

	// Transaction 3: Try to access locked block
	tx3 := NewTransaction(fm, lm, bm)
	assert.NoError(t, tx3.Pin(blk))
	_, err = tx3.GetInt(blk, 80)
	assert.Contains(t, err.Error(), "unable to acquire")
	tx3.Rollback()
	tx2.Commit()

	tx4 := NewTransaction(fm, lm, buffer.NewBufferMgr(fm, lm, 3))
	assert.NoError(t, tx4.Pin(blk))

	ival, err = tx4.GetInt(blk, 80)
	assert.NoError(t, err)
	assert.Equal(t, newIVal, ival)

	sval, err = tx4.GetString(blk, 40)
	assert.NoError(t, err)
	assert.Equal(t, newSVal, sval)

	// Modify and rollback
	assert.NoError(t, tx4.SetInt(blk, 80, 9999, true))
	ival, _ = tx4.GetInt(blk, 80)
	fmt.Printf("Pre-rollback value at location 80 = %d\n", ival)
	tx4.Rollback()

	tx5 := NewTransaction(fm, lm, buffer.NewBufferMgr(fm, lm, 3))
	assert.NoError(t, tx5.Pin(blk))

	ival, _ = tx5.GetInt(blk, 80)
	assert.Equal(t, newIVal, ival)
	fmt.Printf("Post-rollback value at location 80 = %d\n", ival)
	tx5.Commit()

	// Concurrent transaction simulation
	var wg sync.WaitGroup
	blk2 := file.NewBlockId(testFileName, 2)
	blk3 := file.NewBlockId(testFileName, 3)

	wg.Add(4)
	var tx6, tx7, tx8, tx9 *Transaction
	go func() {
		// TX6: W_2 / W_3 => wait (max /3 * 2) => commit
		defer wg.Done()
		tx6 = NewTransaction(fm, lm, bm)
		assert.NoError(t, tx6.Pin(blk2))
		assert.NoError(t, tx6.Pin(blk3))
		assert.NoError(t, tx6.SetInt(blk2, 80, 1, false))
		assert.NoError(t, tx6.SetInt(blk3, 80, 1, false))
		time.Sleep(MAX_TIME / 3 * 2)
		tx6.Rollback()
	}()
	time.Sleep(MAX_TIME / 10) // sleep to force order

	go func() {
		// TX7: R_2 / W_3  => wait (max / 2) + for TX6 to finish => commit
		defer wg.Done()

		tx7 = NewTransaction(fm, lm, bm)
		assert.NoError(t, tx7.Pin(blk3))
		assert.NoError(t, tx7.Pin(blk2))
		assert.NoError(t, tx7.SetInt(blk3, 80, 1, false))
		_, err := tx7.GetInt(blk2, 80)
		assert.NoError(t, err)
		time.Sleep(MAX_TIME)
		tx7.Commit()
	}()
	time.Sleep(MAX_TIME / 10) // sleep to force order
	go func() {
		// TX8: R_3  =>wait (max /3) => timeout waiting for TX7
		defer wg.Done()
		tx8 = NewTransaction(fm, lm, bm)
		assert.NoError(t, tx8.Pin(blk3))
		_, err := tx8.GetInt(blk3, 80)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unable to acquire")
		assert.True(t, tx7.concurMgr.CurrentLocks[*blk3] == X_LOCK)

		tx8.Rollback()
	}()
	time.Sleep(MAX_TIME / 10) // sleep to force order
	go func() {
		// TX9: R_2 => wait (max /2) + for TX6 => hold alongside TX7
		defer wg.Done()
		tx9 = NewTransaction(fm, lm, bm)
		time.Sleep(MAX_TIME / 2)
		assert.NoError(t, tx9.Pin(blk2))
		_, err := tx9.GetInt(blk2, 80)
		// asserts S_LOCK hold by tx7 and tx9 concurrently
		assert.True(t, tx9.concurMgr.CurrentLocks[*blk2] == S_LOCK && tx7.concurMgr.CurrentLocks[*blk2] == S_LOCK)
		assert.NoError(t, err)
		tx9.Commit()
	}()

	wg.Wait()
}
