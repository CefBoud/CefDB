package plan

import (
	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/tx"
)

type QueryPlanner interface {
	CreatePlan(data *parser.QueryData, Transaction *tx.Transaction) (Plan, error)
}
