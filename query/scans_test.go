package query

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
	"github.com/stretchr/testify/assert"
)

// var _ UpdateScan = &record.TableScan{}

func TestScans(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestScans")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 70)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 3)

	// Transaction 1: Write data without logging
	tx1 := tx.NewTransaction(fm, lm, bm)

	s := record.NewSchema()
	s.AddIntField("A")
	s.AddStringField("B", 20)
	l := record.NewLayout(s)

	ts, _ := record.NewTableScan(tx1, "MaTable", l) //NewRecordPage(tx1, blk, l)

	for i := 0; i < 3; i++ {
		assert.NoError(t, ts.Insert())
		// fmt.Printf("InsertAfter %v\n", i)
		ts.SetInt("A", i)
		ts.SetString("B", fmt.Sprintf("record%v", i))
	}
	tx1.Commit()

	e1 := NewFieldExpression("A")
	e2 := NewConstantExpression(1)
	term1 := NewTerm(e1, e2)

	e3 := NewFieldExpression("B")
	e4 := NewConstantExpression("record1")
	term2 := NewTerm(e3, e4)

	p1 := NewPredicate(term1)
	p2 := NewPredicate(term2)
	p1.ConjoinWith(p2)
	ss := NewSelectScan(ts, p1)
	ss.BeforeFirst()

	var actuals_A []int
	var actuals_B []string
	for ss.Next() {
		a, _ := ss.GetInt("A")
		actuals_A = append(actuals_A, a)
		b, _ := ss.GetString("B")
		actuals_B = append(actuals_B, b)
		fmt.Printf("Get  A=%v B=%v\n", a, b)
	}
	assert.Equal(t, []int{1}, actuals_A)
	assert.Equal(t, []string{"record1"}, actuals_B)

	tx2 := tx.NewTransaction(fm, lm, bm)

	s = record.NewSchema()
	s.AddIntField("C")
	s.AddStringField("D", 20)
	l = record.NewLayout(s)
	ts2, _ := record.NewTableScan(tx2, "MaTable2", l)

	for i := 0; i < 2; i++ {
		assert.NoError(t, ts2.Insert())
		fmt.Printf("InsertAfter t2 %v\n", i)
		ts2.SetInt("C", i)
		ts2.SetString("D", fmt.Sprintf("record%v", i))
	}
	tx2.Commit()
	ps := NewProductScan(ts, ts2)
	ps.BeforeFirst()

	actuals_A = make([]int, 0)
	actuals_B = make([]string, 0)
	actuals_C := make([]int, 0)
	actuals_D := make([]string, 0)

	for ps.Next() {
		a, _ := ps.GetInt("A")
		actuals_A = append(actuals_A, a)
		b, _ := ps.GetString("B")
		actuals_B = append(actuals_B, b)
		c, _ := ps.GetInt("C")
		actuals_C = append(actuals_C, c)
		d, _ := ps.GetString("D")
		actuals_D = append(actuals_D, d)
		fmt.Printf("Get  A=%v B=%v C=%v D=%v\n", a, b, c, d)
	}

	assert.Equal(t, []int{0, 0, 1, 1, 2, 2}, actuals_A)
	assert.Equal(t, []string{"record0", "record0", "record1", "record1", "record2", "record2"}, actuals_B)
	assert.Equal(t, []int{0, 1, 0, 1, 0, 1}, actuals_C)
	assert.Equal(t, []string{"record0", "record1", "record0", "record1", "record0", "record1"}, actuals_D)

}
