package parser

import (
	"fmt"
	"strings"

	"github.com/CefBoud/CefDB/query"
)

type QueryData struct {
	Fields    []string
	TableList []string
	Predicate *query.Predicate
}

// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
func (p *Parser) Query(s string) (*QueryData, error) {
	s = toLowerExceptQuotes(s)
	qd := &QueryData{}
	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if !currentTokenIsKeyword(stream, "select") {
		return nil, fmt.Errorf("Query must start with 'select'")
	}
	stream.GoNext()

	fields, err := p.SelectList(stream)
	if err != nil {
		return nil, fmt.Errorf("error parsing  <SelectList> %v", err)
	}
	qd.Fields = fields
	if !currentTokenIsKeyword(stream, "from") {
		return nil, fmt.Errorf("'from' not found after <SelectList> in 'select'")
	}
	stream.GoNext()

	tables, err := p.TableList(stream)
	if err != nil {
		return nil, fmt.Errorf("error parsing  <SelectList> %v", err)
	}
	qd.TableList = tables

	if currentTokenIsKeyword(stream, "where") {
		stream.GoNext()
		p, err := p.Predicate(stream)
		if err != nil {
			return nil, fmt.Errorf("error parsing Predicate after 'where': %v", err)
		}
		qd.Predicate = p
	}

	return qd, nil
}

func (qd *QueryData) String() string {
	if qd == nil {
		return "<nil>"
	}

	var sb strings.Builder

	sb.WriteString("SELECT ")
	if len(qd.Fields) > 0 {
		sb.WriteString(strings.Join(qd.Fields, ", "))
	} else {
		sb.WriteString("*")
	}

	if len(qd.TableList) > 0 {
		sb.WriteString(" FROM ")
		sb.WriteString(strings.Join(qd.TableList, ", "))
	}

	if qd.Predicate != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(qd.Predicate.String())
	}

	return sb.String()
}
