package buffer

import (
	"sync"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

// Buffer manages a page of data in memory, associated with a disk block.
type Buffer struct {
	fm       *file.FileMgr
	lm       *log.LogMgr
	contents *file.Page
	blk      *file.BlockId
	pins     int
	txnum    int
	lsn      int // lsn of the most recent related log record
	sync.Mutex
}

// NewBuffer creates a new Buffer.
func NewBuffer(fm *file.FileMgr, lm *log.LogMgr) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		txnum:    -1,
		lsn:      -1,
	}
}

// Contents returns the Page held by the buffer.
func (b *Buffer) Contents() *file.Page {
	return b.contents
}

// Block returns the BlockId associated with this buffer.
func (b *Buffer) Block() *file.BlockId {
	return b.blk
}

// SetModified marks the buffer as modified by a transaction and optionally sets the LSN.
func (b *Buffer) SetModified(txnum int, lsn int) {
	b.Lock()
	defer b.Unlock()
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// IsPinned returns true if the buffer is currently pinned.
func (b *Buffer) IsPinned() bool {
	b.Lock()
	defer b.Unlock()
	return b.pins > 0
}

// ModifyingTx returns the transaction number that last modified the buffer.
func (b *Buffer) ModifyingTx() int {
	b.Lock()
	defer b.Unlock()
	return b.txnum
}

// AssignToBlock reads the contents of the specified block into the buffer.
// If the buffer was dirty, its previous contents are first written to disk.
func (b *Buffer) AssignToBlock(blk *file.BlockId) error {
	b.Lock()
	defer b.Unlock()
	b.Flush()
	b.blk = blk
	err := b.fm.Read(b.blk, b.contents)
	if err != nil {
		return err
	}
	b.pins = 0
	return nil
}

// Flush writes the buffer to its disk block if it is dirty.
func (b *Buffer) Flush() error {
	if b.txnum >= 0 {
		// flush log record first
		err := b.lm.Flush(b.lsn)
		if err != nil {
			return err
		}
		// flush page
		err = b.fm.Write(b.blk, b.contents)
		if err != nil {
			return err
		}
		b.txnum = -1
	}
	return nil
}

// Pin increases the buffer's pin count.
func (b *Buffer) Pin() {
	b.Lock()
	defer b.Unlock()
	b.pins++
}

// Unpin decreases the buffer's pin count.
func (b *Buffer) Unpin() {
	b.Lock()
	defer b.Unlock()
	b.pins--
}
