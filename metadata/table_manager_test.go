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

func TestTableManager(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestTableManager")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 128)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)

	tx1 := tx.NewTransaction(fm, lm, bm)

	tm := NewTableMgr(true, tx1)
	l, err := tm.GetLayout(FieldCatalogName, tx1)
	assert.NoError(t, err, "Failed to GetLayout")
	// fmt.Printf(" %+v \n %+v\n\n", l, tm.fieldCatalogLayout)
	// fmt.Printf(" %+v \n %+v\n", l.Schema, tm.fieldCatalogLayout.Schema)

	assert.True(t, reflect.DeepEqual(tm.fieldCatalogLayout, l))

	l, err = tm.GetLayout(TableCatalogName, tx1)
	assert.NoError(t, err, "Failed to GetLayout")
	assert.True(t, reflect.DeepEqual(tm.tableCatalogLayout, l))
}
