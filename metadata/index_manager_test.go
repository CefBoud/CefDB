package metadata

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/CefBoud/CefDB/tx"
	"github.com/stretchr/testify/assert"
)

func TestIndexManager(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestIndexManager")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 128)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)

	tx1 := tx.NewTransaction(fm, lm, bm)

	tm := NewTableMgr(true, tx1)

	im, err := NewIndexMgr(true, tm, tx1)
	assert.NoError(t, err, "NewIndexMgr failed")

	imLayout, err := tm.GetLayout(IndexCatalogName, tx1)
	// fmt.Printf(" %+v \n %+v\n\n", im.layout, imLayout)
	assert.NoError(t, err, "NewIndexMgr failed")
	assert.True(t, reflect.DeepEqual(im.layout, imLayout))
}
