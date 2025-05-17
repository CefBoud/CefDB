package parser

import (
	"fmt"

	"github.com/CefBoud/CefDB/query"
	"github.com/bzick/tokenizer"
)

type DeleteData struct {
	Table     string
	Predicate *query.Predicate
}

// <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
func (p *Parser) Delete(s string) (*DeleteData, error) {
	s = toLowerExceptQuotes(s)
	deleteData := &DeleteData{}
	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if !currentTokenIsKeyword(stream, "delete") {
		return nil, fmt.Errorf("Delete must start with 'delete'")
	}
	stream.GoNext()

	if !currentTokenIsKeyword(stream, "from") {
		return nil, fmt.Errorf("expected from in 'delete'")
	}
	stream.GoNext()

	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		deleteData.Table = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing table name in update")
	}

	if currentTokenIsKeyword(stream, "where") {
		stream.GoNext()
		p, err := p.Predicate(stream)
		if err != nil {
			return nil, fmt.Errorf("error parsing Predicate after 'where': %v", err)
		}
		deleteData.Predicate = p
	}
	return deleteData, nil
}

func (ud *DeleteData) String() string {
	return fmt.Sprintf("DeleteData{table: <%v>, predicate: <%v>}", ud.Table, ud.Predicate.String())
}
