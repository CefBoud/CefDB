package buffer

import (
	"time"

	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/viney-shih/go-lock"
)

const MAX_TIME = 3 * time.Second

type BufferMgr struct {
	bufferpool   []*Buffer
	numAvailable int
	mu           *lock.CASMutex
}

// Creates a buffer manager having the specified number
// of buffer slots.
func NewBufferMgr(fm *file.FileMgr, lm *log.LogMgr, numbuffs int) *BufferMgr {
	bufferpool := make([]*Buffer, numbuffs)
	for i := 0; i < numbuffs; i++ {
		bufferpool[i] = NewBuffer(fm, lm)
	}
	bm := &BufferMgr{
		bufferpool:   bufferpool,
		numAvailable: numbuffs,
		mu:           lock.NewCASMutex(),
	}
	// bm.cond = sync.NewCond(&bm.mu)
	return bm
}

// FlushAll flushes the dirty buffers modified by the specified transaction.
func (bm *BufferMgr) FlushAll(txnum int) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	for _, buff := range bm.bufferpool {
		if buff.ModifyingTx() == txnum {
			buff.Flush()
		}
	}
}

// Unpins the specified data buffer
func (bm *BufferMgr) Unpin(buff *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	buff.Unpin()
}

func (bm *BufferMgr) Pin(blk *file.BlockId) *Buffer {
	// fmt.Printf("start Pin %+v\n", blk)
	deadline := time.Now().Add(MAX_TIME)
	// fmt.Printf("TryLockWithTimeout ok? %+v\n", ok)
	for {
		// fmt.Printf("Trying to get pin for %v ", blk)
		if time.Now().After(deadline) {
			return nil
		}
		gotLock := bm.mu.TryLockWithTimeout(time.Until(deadline))
		// fmt.Printf("Pin get lock for %v, ok? %+v\n", blk, ok)
		if gotLock {
			b := bm.tryPin(blk)
			bm.mu.Unlock()
			if b != nil {
				return b
			}
		} else {
			// fmt.Printf("Block %v timed out", blk)
			return nil
		}

	}
}

// tryPin tries to pin a buffer to the specified block.
// If there is already a buffer assigned to that block
// then that buffer is used;
// otherwise, an unpinned buffer from the pool is chosen.
// Returns a null value if there are no available buffers.
func (bm *BufferMgr) tryPin(blk *file.BlockId) *Buffer {
	b := bm.findExistingBuffer(blk)
	if b == nil {
		b = bm.chooseUnpinnedBuffer()
		if b == nil {
			return nil
		}
		b.AssignToBlock(blk)
	}

	if !b.IsPinned() {
		bm.numAvailable--
	}
	b.Pin()
	return b
}

func (bm *BufferMgr) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, b := range bm.bufferpool {
		if b.blk != nil && *b.blk == *blk {
			return b
		}
	}
	return nil
}

func (bm *BufferMgr) chooseUnpinnedBuffer() *Buffer {
	// naive buffer search, return first unpinned.
	// Alternatives: LRU, FIFO ...
	for _, b := range bm.bufferpool {
		if !b.IsPinned() {
			return b
		}
	}
	return nil
}
