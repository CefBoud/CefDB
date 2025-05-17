package parser

import (
	"fmt"

	"github.com/bzick/tokenizer"
)

type InsertData struct {
	Table  string
	Fields []string
	Values []any
}

// <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
func (p *Parser) Insert(s string) (*InsertData, error) {
	s = toLowerExceptQuotes(s)
	insertData := &InsertData{}
	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if !currentTokenIsKeyword(stream, "insert") {
		return nil, fmt.Errorf("Query must start with 'insert'")
	}
	stream.GoNext()
	if !currentTokenIsKeyword(stream, "into") {
		return nil, fmt.Errorf("expecting 'into' but found '%v'", stream.CurrentToken().ValueString())
	}
	stream.GoNext()

	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		insertData.Table = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing table name in insert")
	}

	if stream.CurrentToken().ValueString() != "(" {
		return nil, fmt.Errorf("expected '(' before FieldList in insert")
	}
	stream.GoNext()

	fields, err := p.FieldList(stream)
	if err != nil {
		return nil, fmt.Errorf("error parsing  <FieldList> in 'insert' %v", err)
	}
	insertData.Fields = fields

	if stream.CurrentToken().ValueString() != ")" {
		return nil, fmt.Errorf("expected ')' after FieldList in insert")
	}
	stream.GoNext()

	if stream.CurrentToken().ValueString() != "values" {
		return nil, fmt.Errorf("expected 'values' after FieldList in insert")
	}
	stream.GoNext()
	if stream.CurrentToken().ValueString() != "(" {
		return nil, fmt.Errorf("expected '(' before Constlist in insert")
	}
	stream.GoNext()
	cl, err := p.ConstList(stream)

	if err != nil {
		return nil, fmt.Errorf("error parsing Constlist in insert: %v", err)
	}
	insertData.Values = cl

	if stream.CurrentToken().ValueString() != ")" {
		return nil, fmt.Errorf("expected ')' after Constlist in insert")
	}
	return insertData, nil
}
