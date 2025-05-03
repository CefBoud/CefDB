package buffer

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

func TestBufferMgr(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "buffer")
	os.RemoveAll(tempDir) // clean up any previous runs

	testFileName := "testfile"
	fm, err := file.NewFileMgr(tempDir, 128)

	if err != nil {
		t.Fatalf("Failed to create FileMgr: %v", err)
	}

	logMgr, err := log.NewLogMgr(fm, "testlogfile")
	if err != nil {
		t.Fatalf("Failed to create LogMgr: %v", err)
	}

	for i := 0; i < 50; i++ {
		fm.Append(testFileName)
	}

	bm := NewBufferMgr(fm, logMgr, 3)
	var buff [20]*Buffer

	// Pin blocks
	buff[0] = bm.Pin(file.NewBlockId(testFileName, 0))
	buff[1] = bm.Pin(file.NewBlockId(testFileName, 1))
	buff[2] = bm.Pin(file.NewBlockId(testFileName, 2))

	// Unpin block 1
	bm.Unpin(buff[1])
	buff[1] = nil

	// Pin blocks 0 and 1 again
	buff[3] = bm.Pin(file.NewBlockId(testFileName, 0)) // block 0 pinned twice
	buff[4] = bm.Pin(file.NewBlockId(testFileName, 1)) // block 1 repinned

	// Try to pin block 3 when no buffers are available
	buff[5] = bm.Pin(file.NewBlockId(testFileName, 3))
	if buff[5] != nil {
		t.Errorf("Expected no available buffer for block 3, but it was pinned")
	}
	//  remove second pin of block 0
	bm.Unpin(buff[3])

	// there are no free buffers at this point
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		buff[6] = bm.Pin(file.NewBlockId(testFileName, 4))
	}()
	go func() {
		defer wg.Done()
		buff[7] = bm.Pin(file.NewBlockId(testFileName, 5))
	}()

	go func() {
		defer wg.Done()
		buff[8] = bm.Pin(file.NewBlockId(testFileName, 6))
	}()

	go func() {
		defer wg.Done()
		buff[9] = bm.Pin(file.NewBlockId(testFileName, 7))
	}()

	go func() {
		defer wg.Done()
		bm.Unpin(buff[0]) // one of the three pins above should succeed
		time.Sleep(MAX_TIME / 2)
		bm.Unpin(buff[2]) // then another one
		time.Sleep(MAX_TIME / 2)
		bm.Unpin(buff[4]) // too late, last pin requests should time out

	}()

	wg.Wait()
	actualNils := 0
	expectedNils := 2
	for i := 6; i < 10; i++ {
		if buff[i] == nil {
			actualNils++
		}
	}
	// fmt.Printf("%+v", buff)

	if actualNils != expectedNils {
		t.Errorf("Expected %v pin to fail but %v failed", expectedNils, actualNils)
	}

}
