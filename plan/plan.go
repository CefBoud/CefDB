package plan

import (
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
)

type Plan interface {
	Open() (query.Scan, error)
	BlocksAccessed() int
	RecordsOutput() int
	DistinctValues(fldname string) int
	Schema() *record.Schema
}
