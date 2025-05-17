package plan

import (
	"github.com/CefBoud/CefDB/parser"
	"github.com/CefBoud/CefDB/tx"
)

type UpdatePlanner interface {
	ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) (int, error)
	ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) (int, error)
	ExecuteModify(data *parser.UpdateData, tx *tx.Transaction) (int, error)
	ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) (int, error)
	// ExecuteCreateView(data *parser.CreateViewData, tx *tx.Transaction) (int, error)
	// ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) (int, error)
}
