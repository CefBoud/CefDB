package plan

import (
	"github.com/CefBoud/CefDB/query"
	"github.com/CefBoud/CefDB/record"
)

type SelectPlan struct {
	Plan      Plan
	Predicate *query.Predicate
}

func NewSelectPlan(plan Plan, predicate *query.Predicate) *SelectPlan {
	return &SelectPlan{Plan: plan, Predicate: predicate}
}

func (sp *SelectPlan) Open() (query.Scan, error) {
	s, err := sp.Plan.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(s.(query.UpdateScan), sp.Predicate), nil
}
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.Plan.BlocksAccessed()
}
func (sp *SelectPlan) RecordsOutput() int {
	return sp.Plan.RecordsOutput() / sp.Predicate.ReductionFactor()
}
func (sp *SelectPlan) DistinctValues(fldname string) int {
	// if fldname is equivalent to a constant, we return 1
	if sp.Predicate.EquatesWithConstant(fldname) != nil {
		return 1
	}
	fieldNameDistint := sp.Plan.DistinctValues(fldname)

	// if fldname is equivalent to another field, we return the minimum between  fieldNameDistint and  fieldName2Distinct
	fieldName2 := sp.Predicate.EquatesWithField(fldname)
	if fieldName2 != "" {
		fieldName2Distinct := sp.Plan.DistinctValues(fieldName2)
		if fieldNameDistint > fieldName2Distinct {
			return fieldName2Distinct
		}
	}

	return fieldNameDistint
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.Plan.Schema()
}
