package plan

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/CefBoud/CefDB/buffer"
	"github.com/CefBoud/CefDB/file"
	"github.com/CefBoud/CefDB/log"
	"github.com/CefBoud/CefDB/metadata"
	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/record"
	"github.com/CefBoud/CefDB/tx"
	"github.com/stretchr/testify/assert"
)

func TestQueryPlanner(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestQueryPlanner")
	_ = os.RemoveAll(tempDir) // Clean any previous test data
	fm, err := file.NewFileMgr(tempDir, 256)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 10)

	tx1 := tx.NewTransaction(fm, lm, bm)
	md := metadata.NewMetadataMgr(true, tx1)

	s := record.NewSchema()
	s.AddIntField("a")
	s.AddIntField("b")
	s.AddStringField("c", 20)
	l := record.NewLayout(s)
	md.CreateTable("matable", l.Schema, tx1)

	ts, _ := record.NewTableScan(tx1, "matable", l)
	for i := 0; i < 11; i++ {
		assert.NoError(t, ts.Insert())
		ts.SetInt("a", i)
		ts.SetInt("b", i%2)
		ts.SetString("c", fmt.Sprintf("record%v", i))
		fmt.Printf("inserting i %v\n", i)
	}
	tx1.Commit()
	ts.Close()

	tx1 = tx.NewTransaction(fm, lm, bm)

	queryString := "Select a,b,c from matable where b=0"
	p := parser.NewParser()
	qd, err := p.Query(queryString)
	assert.NoError(t, err, "Select parsing failed")
	myPlan, err := NewBasicQueryPlan(md).CreatePlan(qd, tx1)
	assert.NoError(t, err, "CreatePlan failed")

	scan, err := myPlan.Open()
	assert.NoError(t, err, "myPlan.Open() failed")
	scan.BeforeFirst()

	fmt.Printf("Parsed Query: %v   \n", qd)
	fmt.Printf("myPlan: %t %v   \n", myPlan, myPlan)
	fmt.Printf("scan: %t   \n", scan)
	var actuals_A []int
	var actuals_B []int
	var actuals_C []string
	for scan.Next() {
		a, _ := scan.GetInt("a")
		actuals_A = append(actuals_A, a)
		b, _ := scan.GetInt("b")
		actuals_B = append(actuals_B, b)
		c, _ := scan.GetString("c")
		actuals_C = append(actuals_C, c)
		fmt.Printf("Get  A=%v B=%v C=%v\n", a, b, c)
	}

	assert.Equal(t, []int{0, 2, 4, 6, 8, 10}, actuals_A)
	assert.Equal(t, []int{0, 0, 0, 0, 0, 0}, actuals_B)
	assert.Equal(t, []string{"record0", "record2", "record4", "record6", "record8", "record10"}, actuals_C)

}
