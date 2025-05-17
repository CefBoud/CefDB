package tx

import (
	"fmt"
	"sync/atomic"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
)

// Transaction provides transaction management for clients,
// ensuring that all transactions are serializable and recoverable.
type Transaction struct {
	recoveryMgr *RecoveryMgr
	concurMgr   *ConcurrencyMgr
	bm          *buffer.BufferMgr
	fm          *file.FileMgr
	txnum       int
	mybuffers   map[file.BlockId]*buffer.Buffer
	myPins      map[file.BlockId]int
}

var nextTxNum int64

// This is a dummy block number to lock the EOF
// the goal is to ensure serializability by avoiding phantoms (unaccounted for appends)
const endOfFile = -1

// NewTransaction creates a new transaction and its associated
// recovery and concurrency managers.
// This constructor depends on the file, log, and buffer
// managers that it gets from the class
// simpledb.server.SimpleDB (not directly represented here).
// Those objects are assumed to be initialized elsewhere.
func NewTransaction(fm *file.FileMgr, lm *log.LogMgr, bm *buffer.BufferMgr) *Transaction {

	txnum := atomic.AddInt64(&nextTxNum, 1)

	tx := &Transaction{
		fm:        fm,
		bm:        bm,
		txnum:     int(txnum),
		concurMgr: NewConcurrencyMgr(),
		mybuffers: make(map[file.BlockId]*buffer.Buffer),
		myPins:    make(map[file.BlockId]int),
	}

	tx.recoveryMgr = NewRecoveryMgr(tx, lm, bm)
	return tx
}

// Commit the current transaction.
// Flush all modified buffers (and their log records),
// write and flush a commit record to the log,
// release all locks, and unpin any pinned buffers.
func (tx *Transaction) Commit() {
	tx.recoveryMgr.Commit()
	// fmt.Printf("transaction %d committed\n", tx.txnum)
	tx.concurMgr.Release()
	tx.UnpinAll()
}

// Rollback the current transaction.
// Undo any modified values,
// flush those buffers,
// write and flush a rollback record to the log,
// release all locks, and unpin any pinned buffers.
func (tx *Transaction) Rollback() {
	tx.recoveryMgr.Rollback()
	// fmt.Printf("transaction %d rolled back\n", tx.txnum)
	tx.concurMgr.Release()
	tx.UnpinAll()
}

// Unpin any buffers still pinned by this transaction.
func (tx *Transaction) UnpinAll() {
	for b := range tx.mybuffers {
		tx.Unpin(&b)
	}
}

// Recover flushes all modified buffers.
// Then go through the log, rolling back all
// uncommitted transactions. Finally,
// write a quiescent checkpoint record to the log.
// This method is called during system startup,
// before user transactions begin.
func (tx *Transaction) Recover() {
	tx.bm.FlushAll(tx.txnum)
	tx.recoveryMgr.Recover()
}

// Pin the specified block.
// The transaction manages the buffer for the client.
func (tx *Transaction) Pin(blk *file.BlockId) error {
	buff := tx.bm.Pin(blk)
	if buff == nil {
		return fmt.Errorf("transaction failed to pin block %v ", blk)
	}
	tx.myPins[*blk]++
	tx.mybuffers[*blk] = buff
	return nil
}

// Unpin the specified block.
// The transaction looks up the buffer pinned to this block,
// and unpins it.
func (tx *Transaction) Unpin(blk *file.BlockId) {
	b, ok := tx.mybuffers[*blk]
	if !ok {
		return
	}
	tx.bm.Unpin(b)
	if tx.myPins[*blk] > 0 {
		tx.myPins[*blk]--
	}
	if tx.myPins[*blk] == 0 {
		delete(tx.mybuffers, *blk)
	}

}

// GetInt returns the integer value stored at the
// specified offset of the specified block.
// The method first obtains an SLock on the block,
// then it calls the buffer to retrieve the value.
func (tx *Transaction) GetInt(blk *file.BlockId, offset int) (int, error) {
	ok := tx.concurMgr.SLock(blk)
	// fmt.Printf("tx %v trying to Slock blk %v.  success: %v \n", tx.txnum, blk, ok)
	if !ok {
		return 0, fmt.Errorf("unable to acquire Slock for %v", blk)
	}

	buff := tx.mybuffers[*blk]
	return buff.Contents().GetInt(offset), nil
}

// GetString returns the string value stored at the
// specified offset of the specified block.
// The method first obtains an SLock on the block,
// then it calls the buffer to retrieve the value.
func (tx *Transaction) GetString(blk *file.BlockId, offset int) (string, error) {
	tx.concurMgr.SLock(blk)
	ok := tx.concurMgr.SLock(blk)
	if !ok {
		return "", fmt.Errorf("unable to acquire Slock for %v", blk)
	}
	buff := tx.mybuffers[*blk]
	return buff.Contents().GetString(offset), nil
}

// SetInt stores an integer at the specified offset
// of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and
// writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's id.
func (tx *Transaction) SetInt(blk *file.BlockId, offset int, val int, okToLog bool) error {
	ok := tx.concurMgr.XLock(blk)
	// fmt.Printf("tx %v trying to Xlock blk %v.  success: %v \n", tx.txnum, blk, ok)

	if !ok {
		return fmt.Errorf("unable to acquire Xlock for %v", blk)
	}
	buff := tx.mybuffers[*blk]
	lsn := -1
	var err error
	if okToLog {
		lsn, err = tx.recoveryMgr.SetInt(buff, offset, val)
		if err != nil {
			return fmt.Errorf("unable to write SetInt log record: %v", err)
		}
	}
	p := buff.Contents()
	p.SetInt(offset, val)
	buff.SetModified(tx.txnum, lsn)
	return nil
}

// SetString stores a string at the specified offset
// of the specified block.
// The method first obtains an XLock on the block.
// It then reads the current value at that offset,
// puts it into an update log record, and
// writes that record to the log.
// Finally, it calls the buffer to store the value,
// passing in the LSN of the log record and the transaction's id.
func (tx *Transaction) SetString(blk *file.BlockId, offset int, val string, okToLog bool) error {
	ok := tx.concurMgr.XLock(blk)
	if !ok {
		return fmt.Errorf("unable to acquire Xlock for %v", blk)
	}
	buff := tx.mybuffers[*blk]
	lsn := -1
	var err error
	if okToLog {
		lsn, err = tx.recoveryMgr.SetString(buff, offset, val)
		if err != nil {
			return fmt.Errorf("unable to write SetString log record: %v", err)
		}

	}
	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(tx.txnum, lsn)
	return nil
}

// Size returns the number of blocks in the specified file.
// This method first obtains an SLock on the
// "end of the file", before asking the file manager
// to return the file size.
func (tx *Transaction) Size(filename string) (int, error) {
	dummyblk := file.NewBlockId(filename, endOfFile)
	ok := tx.concurMgr.SLock(dummyblk)
	if !ok {
		return 0, fmt.Errorf("unable to acquire Slock for %v", dummyblk)
	}
	return tx.fm.Length(filename)
}

// Append a new block to the end of the specified file
// and returns a reference to it.
// This method first obtains an XLock on the
// "end of the file", before performing the append.
func (tx *Transaction) Append(filename string) (*file.BlockId, error) {
	dummyblk := file.NewBlockId(filename, endOfFile)
	tx.concurMgr.XLock(dummyblk)
	ok := tx.concurMgr.XLock(dummyblk)
	if !ok {
		return nil, fmt.Errorf("unable to acquire Xlock for %v", dummyblk)
	}
	blk, _ := tx.fm.Append(filename)
	return blk, nil
}

// BlockSize returns the block size used by the file manager.
func (tx *Transaction) BlockSize() int {
	return tx.fm.BlockSize()
}
