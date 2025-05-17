package plan

import (
	"fmt"

	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/tx"
)

type Planner struct {
	QueryPlanner  QueryPlanner
	UpdatePlanner UpdatePlanner
}

func NewPlanner(qp QueryPlanner, up UpdatePlanner) *Planner {
	return &Planner{QueryPlanner: qp, UpdatePlanner: up}
}

func (p *Planner) CreateQueryPlan(cmd string, tx *tx.Transaction) (Plan, error) {
	qd, err := parser.NewParser().Query(cmd)
	if err != nil {
		return nil, err
	}
	return p.QueryPlanner.CreatePlan(qd, tx)
}

func (p *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) (int, error) {

	updateCmd, err := parser.NewParser().UpdateCmd(cmd)
	if err != nil {
		return 0, err
	}

	switch updateCmd.(type) {
	case *parser.InsertData:
		return p.UpdatePlanner.ExecuteInsert(updateCmd.(*parser.InsertData), tx)
	case *parser.DeleteData:
		return p.UpdatePlanner.ExecuteDelete(updateCmd.(*parser.DeleteData), tx)
	case *parser.UpdateData:
		return p.UpdatePlanner.ExecuteModify(updateCmd.(*parser.UpdateData), tx)
	case *parser.CreateTableData:
		return p.UpdatePlanner.ExecuteCreateTable(updateCmd.(*parser.CreateTableData), tx)
	default:
		return 0, fmt.Errorf("unknown command type %v", updateCmd)
	}
}
