package parser

import (
	"fmt"
	"strings"

	"github.com/CefBoud/CefDB/query"
	"github.com/bzick/tokenizer"
)

// Parser for the following grammar:
// <Field> := IdTok
// <Constant> := StrTok | IntTok
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [ AND <Predicate> ]
// <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
// <SelectList> := <Field> [ , <SelectList> ]
// <TableList> := IdTok [ , <TableList> ]
// <UpdateCmd> := <Insert> | <Delete> | <Modify> | <Create>
// <Create> := <CreateTable> | <CreateView> | <CreateIndex>
// <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
// <FieldList> := <Field> [ , <FieldList> ]
// <ConstList> := <Constant> [ , <ConstList> ]
// <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
// <Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
// <CreateTable> := CREATE TABLE IdTok ( <FieldDefs> )
// <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
// <FieldDef> := IdTok <TypeDef>
// <TypeDef> := INT | VARCHAR ( IntTok )
// <CreateView> := CREATE VIEW IdTok AS <Query>
// <CreateIndex> := CREATE INDEX IdTok ON IdTok ( <Field> )
type Parser struct {
	lexer *tokenizer.Tokenizer
}

// define custom tokens keys
const (
	TEquality = iota + 1
	TReservedKeyword
	TComma
	TDot
	TMath
	TSingleQuoted
	TDoubleQuoted
)

func NewParser() *Parser {
	p := &Parser{}
	parser := tokenizer.New()
	parser.DefineTokens(TEquality, []string{"<", "<=", "=", ">=", ">", "!="})
	parser.DefineTokens(TReservedKeyword, []string{"select", "from", "insert", "update", "delete", "create", "table", "index", "view", "where", "set", "into", "values"})
	parser.DefineTokens(TComma, []string{","})
	parser.DefineTokens(TDot, []string{"."})
	parser.DefineTokens(TMath, []string{"+", "-", "/", "*", "%"})
	parser.DefineStringToken(TSingleQuoted, `'`, `'`).SetEscapeSymbol(tokenizer.BackSlash)
	parser.DefineStringToken(TDoubleQuoted, `"`, `"`).SetEscapeSymbol(tokenizer.BackSlash)
	parser.AllowKeywordSymbols(tokenizer.Underscore, tokenizer.Numbers)
	p.lexer = parser

	return p
}

func currentTokenIsKeyword(stream *tokenizer.Stream, keyword string) bool {
	return stream.CurrentToken().Is(TReservedKeyword) && stream.CurrentToken().ValueString() == keyword
}
func currentTokenIs(stream *tokenizer.Stream, s string) bool {
	return stream.CurrentToken().ValueString() == s
}

func (p *Parser) SelectList(stream *tokenizer.Stream) ([]string, error) {
	var res []string
	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		res = append(res, stream.CurrentToken().ValueString())
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing <SelectList> in 'select'")
	}

	if stream.CurrentToken().Is(TComma) {
		stream.GoNext()
		r, err := p.SelectList(stream)
		if err != nil {
			return nil, err
		}
		res = append(res, r...)
	}

	return res, nil
}

func (p *Parser) TableList(stream *tokenizer.Stream) ([]string, error) {
	var res []string
	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		res = append(res, stream.CurrentToken().ValueString())
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing <TableList> in 'select'")
	}

	if stream.CurrentToken().Is(TComma) {
		stream.GoNext()
		r, err := p.TableList(stream)
		if err != nil {
			return nil, err
		}
		res = append(res, r...)
	}

	return res, nil
}

func (p *Parser) FieldList(stream *tokenizer.Stream) ([]string, error) {
	var res []string
	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		res = append(res, stream.CurrentToken().ValueString())
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing <FieldList>")
	}
	if stream.CurrentToken().Is(TComma) {
		stream.GoNext()
		r, err := p.TableList(stream)
		if err != nil {
			return nil, err
		}
		res = append(res, r...)
	}
	return res, nil
}

func (p *Parser) ConstList(stream *tokenizer.Stream) ([]any, error) {
	var res []any
	c, err := p.Constant(stream)
	if err != nil {
		return nil, fmt.Errorf("Error parsing Constantlist: %v:", err)
	}
	res = append(res, c)
	if stream.CurrentToken().ValueString() == "," {
		stream.GoNext()
		r, err := p.ConstList(stream)
		if err != nil {
			return nil, fmt.Errorf("Error parsing Constantlist: %v:", err)
		}
		res = append(res, r...)
	}
	return res, nil
}

// <Field> := IdTok
// <Constant> := StrTok | IntTok
// <Expression> := <Field> | <Constant>
// <Term> := <Expression> = <Expression>
// <Predicate> := <Term> [ AND <Predicate> ]
func (p *Parser) Predicate(stream *tokenizer.Stream) (*query.Predicate, error) {
	t, err := p.Term(stream)
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(t)
	if stream.CurrentToken().ValueString() == "and" {
		stream.GoNext()
		pred2, err := p.Predicate(stream)
		if err != nil {
			return nil, fmt.Errorf("failed parsing predicate following AND: %v", err)
		}
		pred.ConjoinWith(pred2)
	}
	return pred, nil
}

func (p *Parser) Term(stream *tokenizer.Stream) (*query.Term, error) {
	e1, err := p.Expression(stream)
	if err != nil {
		return nil, err
	}
	if stream.CurrentToken().ValueString() != "=" {
		return nil, fmt.Errorf("expected '=' in Term")
	}
	stream.GoNext()
	e2, err := p.Expression(stream)
	if err != nil {
		return nil, err
	}
	return query.NewTerm(e1, e2), nil

}
func (p *Parser) Expression(stream *tokenizer.Stream) (*query.Expression, error) {
	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		fieldName := stream.CurrentToken().ValueString()
		stream.GoNext()
		return query.NewFieldExpression(fieldName), nil
	} else {
		c, err := p.Constant(stream)
		if err != nil {
			return nil, err
		}
		return query.NewConstantExpression(c), nil

	}
}

func (p *Parser) Constant(stream *tokenizer.Stream) (any, error) {
	var res any
	if stream.CurrentToken().Is(tokenizer.TokenInteger) {
		res = int(stream.CurrentToken().ValueInt64())
	} else if stream.CurrentToken().Is(tokenizer.TokenString) {
		res = strings.Trim(stream.CurrentToken().ValueString(), "'")
	} else {
		return nil, fmt.Errorf("%v is not a Constant ", stream.CurrentToken().ValueString())
	}
	stream.GoNext()
	return res, nil
}

func (p *Parser) UpdateCmd(s string) (any, error) {
	s = toLowerExceptQuotes(s)
	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if currentTokenIsKeyword(stream, "insert") {
		return p.Insert(s)
	} else if currentTokenIsKeyword(stream, "update") {
		return p.Modify(s)
	} else if currentTokenIsKeyword(stream, "delete") {
		return p.Delete(s)
	} else if currentTokenIsKeyword(stream, "create") {
		return p.Create(s)
	}

	return nil, fmt.Errorf("Unknown command")
}

func (p *Parser) Create(s string) (any, error) {
	// TODO: index + view
	return p.CreateTable(s)
}
