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
	"github.com/CefBoud/CefDB/tx"
	"github.com/stretchr/testify/assert"
)

func TestPlanner(t *testing.T) {
	tempDir := filepath.Join(os.TempDir(), "TestPlanner")
	_ = os.RemoveAll(tempDir) // Clean any previous test data

	fm, err := file.NewFileMgr(tempDir, 256)
	assert.NoError(t, err, "Failed to create FileMgr")

	logFile := "testlogfiletx"

	lm, err := log.NewLogMgr(fm, logFile)
	assert.NoError(t, err, "Failed to create LogMgr")

	bm := buffer.NewBufferMgr(fm, lm, 10)

	tx1 := tx.NewTransaction(fm, lm, bm)
	md := metadata.NewMetadataMgr(true, tx1)

	qp := NewBasicQueryPlan(md)
	up := NewBasicUpdatePlanner(md)
	planner := NewPlanner(qp, up)

	qryCreate := "create table student(sname varchar(30), gradyear int) ;"
	_, err = planner.ExecuteUpdate(qryCreate, tx1)

	insert1 := "INSERT INTO student (sname, gradyear) VALUES ('Alice Johnson', 2025);"
	_, err = planner.ExecuteUpdate(insert1, tx1)
	assert.NoError(t, err, "Failed to ExecuteUpdate")

	insert2 := "INSERT INTO student (sname, gradyear) VALUES ('Brian Smith', 2024);"
	_, err = planner.ExecuteUpdate(insert2, tx1)
	assert.NoError(t, err, "Failed to ExecuteUpdate")

	insert3 := "INSERT INTO student (sname, gradyear) VALUES ('Cynthia Lee', 2026);"
	_, err = planner.ExecuteUpdate(insert3, tx1)
	assert.NoError(t, err, "Failed to ExecuteUpdate")
	tx1.Commit()

	delete1 := "DELETE FROM student where gradyear = 2025);"
	_, err = planner.ExecuteUpdate(delete1, tx1)
	assert.NoError(t, err, "Failed to ExecuteUpdate")
	tx1.Commit()

	queryString := "Select sname,gradyear from student "
	plan, err := planner.CreateQueryPlan(queryString, tx1)
	assert.NoError(t, err, "Failed to CreateQueryPlan")

	s, err := plan.Open()

	assert.NoError(t, err, "Failed to Open")

	var snames []string
	var gradyears []int
	for s.Next() {
		sname, _ := s.GetString("sname")
		snames = append(snames, sname)

		gradeyear, _ := s.GetInt("gradyear")
		gradyears = append(gradyears, gradeyear)

		fmt.Printf("Get  sname=%v gradeyear=%v \n", sname, gradeyear)
	}
	s.Close()
	tx1.Commit()
	assert.Equal(t, []string{"Brian Smith", "Cynthia Lee"}, snames)
	assert.Equal(t, []int{2024, 2026}, gradyears)

}
