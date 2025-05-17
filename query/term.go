package query

import (
	"fmt"

	"github.com/CefBoud/CefDB/record"
)

type Term struct {
	left, right *Expression
}

func NewTerm(e1, e2 *Expression) *Term {
	return &Term{left: e1, right: e2}
}

func (t *Term) IsSatisfied(s Scan) (bool, error) {
	lval, err := t.left.Evaluate(s)
	if err != nil {
		return false, fmt.Errorf("term IsSatisfied error: %v", err)
	}
	rval, err := t.right.Evaluate(s)
	if err != nil {
		return false, fmt.Errorf("term IsSatisfied error: %v", err)
	}
	return lval == rval, nil
}

func (e *Term) AppliesTo(sch *record.Schema) bool {
	return e.left.AppliesTo(sch) && e.right.AppliesTo(sch)
}

func (e *Term) ReductionFactor() {
	// TODO
}

// EquatesWithField takes in a 'fieldName' and returns if another fieldName2
// if Term if `fieldName =fieldName2` or `fieldName2 = fieldName`
func (e *Term) EquatesWithField(fieldName string) string {
	if e.left.FieldName == fieldName && e.right.IsFieldName() {
		return e.right.FieldName
	}
	if e.right.FieldName == fieldName && e.left.IsFieldName() {
		return e.left.FieldName
	}
	return ""
}

func (e *Term) EquatesWithConstant(fieldName string) any {
	if e.left.FieldName == fieldName && !e.right.IsFieldName() {
		return e.right.C
	}
	if e.right.FieldName == fieldName && !e.left.IsFieldName() {
		return e.left.C
	}
	return nil
}

func (t *Term) String() string {
	if t == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s = %s", t.left.String(), t.right.String())
}
