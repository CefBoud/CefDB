package plan

import (
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
)

type ProjectPlan struct {
	Plan   Plan
	schema *record.Schema
}

func NewProjectPlan(plan Plan, fields []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, f := range fields {
		schema.Add(f, plan.Schema())
	}
	return &ProjectPlan{Plan: plan, schema: schema}
}

func (pp *ProjectPlan) Open() (query.Scan, error) {
	s, err := pp.Plan.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, pp.Schema().GetFields()), nil
}

func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.Plan.BlocksAccessed()
}
func (pp *ProjectPlan) RecordsOutput() int {
	return pp.Plan.RecordsOutput()
}
func (pp *ProjectPlan) DistinctValues(fldname string) int {
	return pp.Plan.DistinctValues(fldname)
}

func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
