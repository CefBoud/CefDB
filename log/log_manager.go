package log

import (
	"fmt"
	"sync"

	"github.com/CefBoud/CefDB/file"
)

// LogMgr manages the log file.
type LogMgr struct {
	fm           *file.FileMgr
	logfile      string
	logpage      *file.Page
	currentblk   *file.BlockId
	latestLSN    int
	lastSavedLSN int
	sync.Mutex
}

const INTEGER_BYTES = 4

// NewLogMgr creates a new LogMgr for the specified log file.
// If the log file does not yet exist, it is created with an empty first block.
func NewLogMgr(fm *file.FileMgr, logfile string) (*LogMgr, error) {
	lm := &LogMgr{
		fm:      fm,
		logfile: logfile,
		logpage: file.NewPage(fm.BlockSize()),
	}
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("getting log file length: %w", err)
	}

	if logsize == 0 {
		blk, err := lm.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("appending new block: %w", err)
		}
		lm.currentblk = blk
	} else {
		lm.currentblk = file.NewBlockId(logfile, logsize-1)
		if err := fm.Read(lm.currentblk, lm.logpage); err != nil {
			return nil, fmt.Errorf("reading log page: %w", err)
		}
	}
	return lm, nil
}

// Flush ensures that the log record corresponding to the specified LSN has been written to disk.
// All earlier log records will also be written to disk.
func (lm *LogMgr) Flush(lsn int) error {
	if lsn >= lm.lastSavedLSN {
		return lm.flush()
	}
	return nil
}

// Iterator returns an iterator for the log records in reverse order.
func (lm *LogMgr) Iterator() (*LogIterator, error) {
	if err := lm.flush(); err != nil {
		return nil, err
	}
	return NewLogIterator(lm.fm, lm.currentblk), nil
	// return iter.Iterate(), nil
}

// Append appends a log record to the log buffer.
// The record consists of an arbitrary array of bytes.
// Log records are written right to left in the buffer.
// The size of the record is written before the bytes.
// The beginning of the buffer contains the location
// of the last-written record (the "boundary").
// Storing the records backwards makes it easy to read
// them in reverse order.
// Returns the LSN of the final value.
func (lm *LogMgr) Append(logrec []byte) (int, error) {
	lm.Lock()
	defer lm.Unlock()

	boundary := int(lm.logpage.GetInt(0))
	recsize := len(logrec)
	bytesneeded := recsize + INTEGER_BYTES // Size of int32 for record length

	if boundary-bytesneeded < INTEGER_BYTES { // the log record doesn't fit,
		if err := lm.flush(); err != nil { // so move to the next block.
			return -1, err
		}
		blk, err := lm.appendNewBlock()
		if err != nil {
			return -1, err
		}
		lm.currentblk = blk
		boundary = lm.logpage.GetInt(0)
	}
	recpos := boundary - bytesneeded

	lm.logpage.SetBytes(recpos, logrec)
	lm.logpage.SetInt(0, recpos)

	lm.latestLSN++
	return lm.latestLSN, nil
}

// appendNewBlock initializes a new block for the log file and appends it.
func (lm *LogMgr) appendNewBlock() (*file.BlockId, error) {
	blk, err := lm.fm.Append(lm.logfile)
	if err != nil {
		return nil, fmt.Errorf("appending block to file manager: %w", err)
	}

	lm.logpage.SetInt(0, lm.fm.BlockSize())
	if err := lm.fm.Write(blk, lm.logpage); err != nil {
		return nil, fmt.Errorf("writing new block to file manager: %w", err)
	}
	return blk, nil
}

// flush writes the current log buffer to the log file.
func (lm *LogMgr) flush() error {
	if err := lm.fm.Write(lm.currentblk, lm.logpage); err != nil {
		return fmt.Errorf("writing log page to file manager: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}

// LogIterator provides an iterator to read log records in reverse order.
type LogIterator struct {
	fm          *file.FileMgr
	currentBlk  *file.BlockId
	currentPage *file.Page
	currentPos  int
	blockSize   int
}

// NewLogIterator creates a new LogIterator.
func NewLogIterator(fm *file.FileMgr, currentBlk *file.BlockId) *LogIterator {
	blockSize := fm.BlockSize()
	p := file.NewPage(blockSize)
	fm.Read(currentBlk, p)
	boundary := p.GetInt(0)
	return &LogIterator{
		fm:          fm,
		currentBlk:  currentBlk,
		currentPage: p,
		currentPos:  boundary,
		blockSize:   blockSize,
	}
}

// nextRecord reads the next log record (going backwards).
// Returns nil if there are no more records in the current block.
func (li *LogIterator) NextRecord() []byte {
	if li.currentPos < li.fm.BlockSize() {
		return li.readCurrentRecord()
	} else if li.currentBlk.Blknum > 0 {
		li.currentBlk.Blknum -= 1
		li.fm.Read(li.currentBlk, li.currentPage)
		li.currentPos = li.currentPage.GetInt(0)
		return li.readCurrentRecord()
	}
	return nil
}

func (li *LogIterator) readCurrentRecord() []byte {
	b := li.currentPage.GetBytes(li.currentPos)
	li.currentPos += len(b) + 4
	return b
}
