package parser

import (
	"fmt"

	"github.com/CefBoud/CefDB/record"
	"github.com/bzick/tokenizer"
)

type FieldDef struct {
	Fname string
	FType string // INT | VARCHAR
}
type CreateTableData struct {
	Table  string
	Schema *record.Schema
}

// CREATE TABLE IdTok ( <FieldDefs> )
// <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
func (p *Parser) CreateTable(s string) (*CreateTableData, error) {
	s = toLowerExceptQuotes(s)
	createData := &CreateTableData{}

	stream := p.lexer.ParseString(s)
	defer stream.Close()
	if !currentTokenIsKeyword(stream, "create") {
		return nil, fmt.Errorf("create table must start with 'create'")
	}
	stream.GoNext()

	if !currentTokenIsKeyword(stream, "table") {
		return nil, fmt.Errorf("create table must start with 'create table'")
	}
	stream.GoNext()

	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		createData.Table = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return nil, fmt.Errorf(" error parsing table name in create")
	}
	schema, err := p.FieldDefs(stream)
	if err != nil {
		return nil, fmt.Errorf(" error parsing <FieldDefs> in create table: %v", err)
	}
	createData.Schema = schema
	return createData, nil
}

func (p *Parser) FieldDefs(stream *tokenizer.Stream) (*record.Schema, error) {
	if stream.CurrentToken().ValueString() != "(" {
		return nil, fmt.Errorf("error parsing FieldDefs expected '('")
	}
	stream.GoNext()
	res := make(map[string]record.FieldInfo)
	fname, finfo, err := p.FieldDef(stream)
	if err != nil {
		return nil, fmt.Errorf("error parsing FieldDefs: %v", err)
	}
	res[fname] = finfo

	for stream.CurrentToken().ValueString() == "," {
		stream.GoNext()
		fname, finfo, err := p.FieldDef(stream)
		if err != nil {
			return nil, fmt.Errorf(" error parsing FieldDefs: %v", err)
		}
		res[fname] = finfo
	}
	if stream.CurrentToken().ValueString() != ")" {
		return nil, fmt.Errorf("error parsing FieldDefs expected ')'")
	}
	return record.NewSchemaWithFields(res), nil
}

// <FieldDef> := IdTok <TypeDef>
// <TypeDef> := INT | VARCHAR ( IntTok )
func (p *Parser) FieldDef(stream *tokenizer.Stream) (string, record.FieldInfo, error) {
	var fInfo record.FieldInfo
	var fname string
	if stream.CurrentToken().Is(tokenizer.TokenKeyword) {
		fname = stream.CurrentToken().ValueString()
		stream.GoNext()
	} else {
		return fname, fInfo, fmt.Errorf("error parsing FieldDef. Expected fieldName but got %v", stream.CurrentToken().ValueString())
	}
	if stream.CurrentToken().ValueString() == "int" {
		stream.GoNext()
		fInfo.Type = record.INTEGER
	} else if stream.CurrentToken().ValueString() == "varchar" {
		stream.GoNext()
		fInfo.Type = record.VARCHAR
		if stream.CurrentToken().ValueString() == "(" {
			stream.GoNext()
			if !stream.CurrentToken().IsInteger() {
				return fname, fInfo, fmt.Errorf(" error parsing FieldDef TypeDef varchar: expecting int after '('")
			} else {
				fInfo.Length = int(stream.CurrentToken().ValueInt64())
				stream.GoNext()
				if stream.CurrentToken().ValueString() != ")" {
					return fname, fInfo, fmt.Errorf(" error parsing FieldDef TypeDef varchar: expecting ')' after int")
				}
				stream.GoNext()
			}
		}
	} else {
		return fname, fInfo, fmt.Errorf(" error parsing FieldDef type: expecting int or varchar but got : %v", stream.CurrentToken().ValueString())
	}
	return fname, fInfo, nil
}
