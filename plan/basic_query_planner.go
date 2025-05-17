package plan

import (
	"fmt"

	"github.com/CefBoud/CefDB/metadata"
	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/tx"
)

type BasicQueryPlan struct {
	Md *metadata.MetadataMgr
}

func NewBasicQueryPlan(md *metadata.MetadataMgr) *BasicQueryPlan {
	return &BasicQueryPlan{Md: md}
}

func (bqp *BasicQueryPlan) CreatePlan(data *parser.QueryData, tx *tx.Transaction) (Plan, error) {
	var plan Plan
	var tablePlans []*TablePlan

	for _, table := range data.TableList {
		tp, err := NewTablePlan(table, tx, bqp.Md)
		if err != nil {
			return nil, fmt.Errorf("createPlan NewTablePlan error : %v", err)
		}
		tablePlans = append(tablePlans, tp)
	}
	plan = tablePlans[0]

	for i := 1; i < len(tablePlans); i++ {
		option1 := NewProductPlan(plan, tablePlans[i])
		option2 := NewProductPlan(tablePlans[i], plan)
		// pick the order that minimizes block access
		if option1.BlocksAccessed() > option2.BlocksAccessed() {
			plan = option2
		} else {
			plan = option1
		}
	}

	if data.Predicate != nil {
		plan = NewSelectPlan(plan, data.Predicate)
	}

	plan = NewProjectPlan(plan, data.Fields)

	return plan, nil
}
