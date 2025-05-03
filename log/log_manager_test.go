package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/CefBoud/CefDB/file"
)

func TestLogMgr(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "log")
	os.RemoveAll(tempDir) // clean up any previous runs

	fm, err := file.NewFileMgr(tempDir, 128)

	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}
	logfile := "testlogfile"

	// Create the LogMgr
	logMgr, err := NewLogMgr(fm, logfile)
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	// Data to append
	logRec1 := []byte("First log record, First log record, First log record, First log record, First log record, First log record")
	logRec2 := []byte("Second log record, Second log record, Second log record, Second log record, Second log record")

	// Append first record
	lsn1, err := logMgr.Append(logRec1)
	if err != nil {
		t.Fatalf("Failed to append first record: %v", err)
	}
	if lsn1 != 1 { // 1
		t.Fatalf("Expected LSN1 to be greater 1, got %d", lsn1)
	}

	// Append second record
	lsn2, err := logMgr.Append(logRec2)
	if err != nil {
		t.Fatalf("Failed to append second record: %v", err)
	}
	if lsn2 != 2 {
		t.Fatalf("Expected LSN2 to be 2, got : %d", lsn2)
	}

	// Flush the logs to ensure they're written to disk
	err = logMgr.Flush(lsn2)
	if err != nil {
		t.Fatalf("Failed to flush logs: %v", err)
	}

	logRec3 := []byte("Third log record, Third log record, Third log record, Third log record, Third log record")

	// Append second record
	_, err = logMgr.Append(logRec3)
	if err != nil {
		t.Fatalf("Failed to append third record: %v", err)
	}
	// Read the logs back using an iterator
	iter, err := logMgr.Iterator()
	if err != nil {
		t.Fatalf("Failed to create LogIterator: %v", err)
	}

	// Test reading the logs back in reverse order
	record := iter.NextRecord()
	if record == nil {
		t.Fatal("Expected to read a record, but got nil")
	}
	if string(record) != string(logRec3) {
		t.Fatalf("Read record doesn't match expected third record. Got: %s, Expected: %s", string(record), string(logRec3))
	}

	record = iter.NextRecord()
	if record == nil {
		t.Fatal("Expected to read a record, but got nil")
	}
	if string(record) != string(logRec2) {
		t.Fatalf("Read record doesn't match expected second record. Got: %s, Expected: %s", string(record), string(logRec2))
	}

	record = iter.NextRecord()
	if record == nil {
		t.Fatal("Expected to read a record, but got nil")
	}
	if string(record) != string(logRec1) {
		t.Fatalf("Read record doesn't match expected first record. Got: %s, Expected: %s", string(record), string(logRec1))
	}

	// Ensure no more records are available
	record = iter.NextRecord()
	if record != nil {
		t.Fatalf("Expected no more records after reading all records, but got: %s", string(record))
	}
}
