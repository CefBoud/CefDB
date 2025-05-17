package query

import (
	"fmt"
	"strings"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate(t *Term) *Predicate {
	p := &Predicate{}
	p.terms = append(p.terms, t)
	return p
}
func (p *Predicate) IsSatisfied(s Scan) (bool, error) {
	for _, t := range p.terms {
		satisfied, err := t.IsSatisfied(s)
		if !satisfied {
			return false, fmt.Errorf("Predicate IsSatisfied error: %v", err)
		}
	}
	return true, nil
}

func (p *Predicate) ConjoinWith(p2 *Predicate) {
	p.terms = append(p.terms, p2.terms...)
}

func (p *Predicate) ReductionFactor() int {
	// TODO
	return 2
}

// EquatesWithField checks the Predicates expression and return a field name "fieldNameOutput" (if it exists)
// that if there is a term `fieldName = fieldNameOutput`
func (p *Predicate) EquatesWithField(fieldName string) string {
	for _, t := range p.terms {
		equivalent := t.EquatesWithField(fieldName)
		if equivalent != "" {
			return equivalent
		}
	}
	return ""
}

// EquatesWithField checks the Predicates expression and return a field name "ConstantOutput" (if it exists)
// that if there is a term `fieldName = ConstantOutput`
func (p *Predicate) EquatesWithConstant(fieldName string) any {
	for _, t := range p.terms {
		equivalent := t.EquatesWithConstant(fieldName)
		if equivalent != nil {
			return equivalent
		}
	}
	return nil
}

// TODO:
// selectSubPred
// joinSubPred

func (p *Predicate) String() string {
	if p == nil {
		return "<nil>"
	}
	var sb strings.Builder
	for i, term := range p.terms {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString(term.String())
	}
	return sb.String()
}
