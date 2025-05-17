package plan

import (
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
)

type ProductPlan struct {
	Left   Plan
	Right  Plan
	schema *record.Schema
}

func NewProductPlan(left Plan, right Plan) *ProductPlan {
	s := left.Schema()
	s.AddAll(right.Schema())
	return &ProductPlan{Left: left, Right: right, schema: s}
}

func (pp *ProductPlan) Open() (query.Scan, error) {
	l, err := pp.Left.Open()
	if err != nil {
		return nil, err
	}
	r, err := pp.Right.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(l, r), nil
}
func (pp *ProductPlan) BlocksAccessed() int {
	return pp.Left.BlocksAccessed() + (pp.Left.RecordsOutput() * pp.Right.BlocksAccessed())
}
func (pp *ProductPlan) RecordsOutput() int {
	return pp.Left.RecordsOutput() * pp.Left.RecordsOutput()
}
func (pp *ProductPlan) DistinctValues(fldname string) int {
	if pp.Left.Schema().HasField(fldname) {
		return pp.Left.DistinctValues(fldname)
	}
	return pp.Right.DistinctValues(fldname)
}

func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
