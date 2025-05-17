package query

import (
	"fmt"

	"github.com/CefBoud/CefDB/record"
)

// type Constant any

// Expressions represents either a constant  or a field in a scan
type Expression struct {
	C         any
	FieldName string
}

func NewConstantExpression(c any) *Expression {
	return &Expression{C: c}
}
func NewFieldExpression(fname string) *Expression {
	return &Expression{FieldName: fname}
}
func (e *Expression) IsFieldName() bool {
	return e.FieldName != ""
}

func (e *Expression) Evaluate(s Scan) (any, error) {
	if e.IsFieldName() {
		return s.GetVal(e.FieldName)
	}
	return e.C, nil
}

func (e *Expression) AppliesTo(sch *record.Schema) bool {
	if e.IsFieldName() {
		return sch.HasField(e.FieldName)
	}
	return true
}

func (e *Expression) String() string {
	if e == nil {
		return "<nil>"
	}
	if e.FieldName != "" {
		return e.FieldName
	}
	return fmt.Sprintf("%v", e.C)
}
