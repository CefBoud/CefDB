package parser

import (
	"fmt"

	"github.com/CefBoud/CefDB/query"
	"github.com/bzick/tokenizer"
)

type UpdateData struct {
	Table      string
	Field      string
	Expression *query.Expression
	Predicate  *query.Predicate
}

// <Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
func (p *Parser) Modify(s string) (*UpdateData, error) {
	s = toLowerExceptQuotes(s)
	updateData := &UpdateData{}
	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if !currentTokenIsKeyword(stream, "update") {
		return nil, fmt.Errorf("Modify must start with 'update'")
	}
	stream.GoNext()

	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		updateData.Table = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing table name in update")
	}

	if !currentTokenIsKeyword(stream, "set") {
		return nil, fmt.Errorf("expecting 'into' but found '%v'", stream.CurrentToken().ValueString())
	}
	stream.GoNext()

	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		updateData.Field = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing field in update")
	}

	if stream.CurrentToken().ValueString() != "=" {
		return nil, fmt.Errorf("expected '=' after set field in update")
	}
	stream.GoNext()

	expr, err := p.Expression(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to parse expression in update")
	}
	updateData.Expression = expr

	if currentTokenIsKeyword(stream, "where") {
		stream.GoNext()
		p, err := p.Predicate(stream)
		if err != nil {
			return nil, fmt.Errorf("error parsing Predicate after 'where': %v", err)
		}
		updateData.Predicate = p
	}
	return updateData, nil
}

func (ud *UpdateData) String() string {
	return fmt.Sprintf("UpdateData{table: <%v>, field: <%v>, expression: <%v>, predicate: <%v>}",
		ud.Table, ud.Field, ud.Expression.String(), ud.Predicate.String())
}
