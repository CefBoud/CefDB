package file

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileManager(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "file")
	os.RemoveAll(tempDir) //clean up any previous runs
	blockSize := 4096

	fm, err := NewFileMgr(tempDir, blockSize)
	if err != nil {
		t.Fatalf("NewFileMgr(%q, %d) error = %v", tempDir, blockSize, err)
	}

	filename := "toto"

	// Append the first block to the file
	blk1, err := fm.Append(filename)
	if err != nil {
		t.Fatalf("fm.Append(%q) error = %v", filename, err)
	}
	// t.Logf("Appended block: %v", blk1)

	// Create a new page and write a string to it
	p1 := NewPage(fm.BlockSize())
	expectedStr := "JOJOJOJOJOJO"
	offsetStr := 42
	p1.SetString(offsetStr, expectedStr)
	if err := fm.Write(blk1, p1); err != nil {
		t.Fatalf("fm.Write(%v, page) error = %v", blk1, err)
	}

	// Read the block back and verify the string
	p2 := NewPage(fm.BlockSize())
	if err := fm.Read(blk1, p2); err != nil {
		t.Fatalf("fm.Read(%v, page) error = %v", blk1, err)
	}
	actualStr := p2.GetString(offsetStr)
	if actualStr != expectedStr {
		t.Errorf("GetString(%d) returned %q, expected %q", offsetStr, actualStr, expectedStr)
	}

	// Append a second block and write an integer to it
	blk2, err := fm.Append(filename)
	if err != nil {
		t.Fatalf("fm.Append(%q) error = %v", filename, err)
	}
	// t.Logf("Appended block: %v", blk2)

	expectedInt := 95
	offsetInt := 200
	p2.SetInt(offsetInt, expectedInt)
	if err := fm.Write(blk2, p2); err != nil {
		t.Fatalf("fm.Write(%v, page) error = %v", blk2, err)
	}

	// Read the second block back and verify the integer
	p3 := NewPage(fm.BlockSize())
	if err := fm.Read(blk2, p3); err != nil {
		t.Fatalf("fm.Read(%v, page) error = %v", blk2, err)
	}
	actualInt := p3.GetInt(offsetInt)
	if actualInt != expectedInt {
		t.Errorf("GetInt(%d) returned %d, expected %d", offsetInt, actualInt, expectedInt)
	}
}
