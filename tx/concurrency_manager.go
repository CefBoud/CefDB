package tx

import (
	"sync"
	"time"

	"github.com/CefBoud/CefDB/file"
	"github.com/viney-shih/go-lock"
)

// global lock map storing blockId => *sync.RWMutex{}
// very important to store pointers as mutex must not be copied
var globalLockMap sync.Map

const (
	MAX_TIME = 5 * time.Second
	S_LOCK   = 1 // read shared lock
	X_LOCK   = 2 // write exclusive lock
)

type ConcurrencyMgr struct {
	CurrentLocks map[file.BlockId]int
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	return &ConcurrencyMgr{
		CurrentLocks: make(map[file.BlockId]int),
	}
}

func (cm *ConcurrencyMgr) SLock(blk *file.BlockId) bool {
	// if we already have a lock (S or X), we return
	if _, ok := cm.CurrentLocks[*blk]; ok {
		return ok
	}

	v, _ := globalLockMap.LoadOrStore(*blk, lock.NewCASMutex())
	l := v.(*lock.CASMutex)
	ok := l.RTryLockWithTimeout(MAX_TIME)
	if ok {
		cm.CurrentLocks[*blk] = S_LOCK
	}
	return ok
}

func (cm *ConcurrencyMgr) XLock(blk *file.BlockId) bool {

	if v, ok := cm.CurrentLocks[*blk]; ok {
		// if we already have a X_LOCK, we return early
		if v == X_LOCK {
			return ok
		}
		// we have a S_LOCK, we release it to upgrade to an X_LOCK
		cm.SUnlock(blk)
	}

	v, _ := globalLockMap.LoadOrStore(*blk, lock.NewCASMutex())
	l := v.(*lock.CASMutex)
	ok := l.TryLockWithTimeout(MAX_TIME)
	if ok {
		cm.CurrentLocks[*blk] = X_LOCK
	}
	return ok
}

func (cm *ConcurrencyMgr) SUnlock(blk *file.BlockId) {
	v, _ := globalLockMap.LoadOrStore(*blk, lock.NewCASMutex())
	l := v.(*lock.CASMutex)
	l.RUnlock()
	delete(cm.CurrentLocks, *blk)
}

func (cm *ConcurrencyMgr) XUnlock(blk *file.BlockId) {
	v, _ := globalLockMap.LoadOrStore(*blk, lock.NewCASMutex())
	l := v.(*lock.CASMutex)
	l.Unlock()
	delete(cm.CurrentLocks, *blk)
}

// Release all locks held by the tx
func (cm *ConcurrencyMgr) Release() {
	for b, l := range cm.CurrentLocks {
		if l == S_LOCK {
			cm.SUnlock(&b)
		} else {
			cm.XUnlock(&b)
		}
	}
}
