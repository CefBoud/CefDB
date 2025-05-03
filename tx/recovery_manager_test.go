package tx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/stretchr/testify/assert"
)

func TestRecoveryMgr(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestRecoveryMgr")
	_ = os.RemoveAll(tempDir) // Clean any previous data

	fm, err := file.NewFileMgr(tempDir, 256)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfile"
	testFileName := "testfile"
	for i := 0; i < 50; i++ {
		fm.Append(testFileName)
	}

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)
	blk := file.NewBlockId(testFileName, 1)

	tx1 := NewTransaction(fm, lm, bm)
	tx1.Pin(blk)
	// init values without logging them
	tx1.SetInt(blk, 40, 1, false)
	tx1.SetString(blk, 100, "Yes!", false)
	// write values and log
	tx1.SetInt(blk, 40, 2, true)
	tx1.SetString(blk, 100, "no!", true)
	tx1.Commit()

	iter, _ := lm.Iterator()
	record := CreateLogRecord(iter.NextRecord())
	assert.Equal(t, record.String(), "LogRecord{TxNum: 1, Op: COMMIT}")

	record = CreateLogRecord(iter.NextRecord())
	assert.Equal(t, record.String(), "LogRecord{TxNum: 1, Op: SETSTRING, FileName: testfile, Blknum: 1, Offset: 100, OldVal: Yes!, NewVal: no!}")
	record = CreateLogRecord(iter.NextRecord())
	assert.Equal(t, record.String(), "LogRecord{TxNum: 1, Op: SETINT, FileName: testfile, Blknum: 1, Offset: 40, OldVal: 1, NewVal: 2}")

	record = CreateLogRecord(iter.NextRecord())
	assert.Equal(t, record.String(), "LogRecord{TxNum: 1, Op: START}")

	// test recovery
	tx2 := NewTransaction(fm, lm, bm)
	tx2.Pin(blk)

	tx2.SetInt(blk, 40, 3, true)
	tx2.SetString(blk, 100, "maybe", true)

	// flush buffers and releases locks and buffers
	bm.FlushAll(tx2.txnum)
	tx2.concurMgr.Release()
	tx2.Unpin(blk)

	tx3 := NewTransaction(fm, lm, bm)
	tx3.Pin(blk)
	ival, _ := tx3.GetInt(blk, 40)
	sval, _ := tx3.GetString(blk, 100)
	tx3.Commit()
	assert.Equal(t, 3, ival)
	assert.Equal(t, "maybe", sval)

	// we recover, all uncommited values (from tx2 should be reverted)

	tx4 := NewTransaction(fm, lm, bm)
	tx4.Recover()

	tx4.Pin(blk)
	ival, _ = tx4.GetInt(blk, 40)
	sval, _ = tx4.GetString(blk, 100)
	tx3.Commit()
	assert.Equal(t, 2, ival)
	assert.Equal(t, "no!", sval)

}
